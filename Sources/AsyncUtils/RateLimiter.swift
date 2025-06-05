//
//  RateLimiter.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 21.05.25.
//  Licensed under the MIT-License included in the project.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

import Foundation
import DequeModule

/// A rate limiter that allows you to control the rate of actions in your application using a leaky bucket or token bucket (like) algorithm.
/// Tokens can be cosumed synchronously or asynchronously.
/// Asynchronous consumption will block until a token is available, while synchronous consumption will return `false` if no tokens are available.
/// Tasks blocked waiting for a token will be resumed in the order they were blocked (FIFO).
/// - Note: This implementation internally floating point arithmetic and asynchronous functions, so it is not suitable for high precision timing. 
public actor RateLimiter {
    /// The maximum number of tokens that can be held in the bucket. For a leaky bucket, this is always 1.
    public var maxTokens: Int
    /// The rate at which tokens are regenerated, expressed in tokens per second.
    public var tokenRate: Double
    
    /// The number of tokens currently available in the bucket, not including tokens that may have been generated but not yet accounted for.
    /// This value is updated whenever tokens are regenerated.
    private var tokensInBucket: Int

    /// The last time tokens were regenerated.
    /// This is used to calculate how many tokens should be available at the current time.
    private var lastTokenRegenerate: Date

    /// The timestamp at which the next token will be regenerated.
    /// This is used to schedule the next regeneration of tokens.
    /// It can be `nil` if the bucket is full or has never generated tokens yet.
    private var nextTokenRegenerate: Date?
    
    /// Since `RateLimiter` shares many similarities with `AsyncSemaphore`, we can use the same `BlockedWaiter` type.
    typealias BlockedWaiter = AsyncSemaphore.BlockedWaiter
    
    /// All current blocked waiters, indexed by their ticket.
    /// This dictionary maps each `Ticket` to its corresponding `BlockedWaiter`.
    /// - Note: This is used to manage the waiters and their continuations.
    /// It allows for cancellation of waiters and resuming their continuations when a permit is signaled.
    private var blockedWaiters: [Ticket: BlockedWaiter] = [:]

    /// A queue of tickets representing the blocked waiters. The queue is used to manage the order in which waiters are signaled when a permit becomes available.
    private var queue: Deque<Ticket> = []

    /// A task that is responsible for regenerating tokens at the specified rate.
    /// This task runs in the background and is scheduled to run when there are blocked waiters.
    /// It will sleep until the next token regeneration time and then regenerate tokens.
    /// If there are no blocked waiters, the task will be cancelled to save resources.
    private var regenerationTask: Task<Void, Never>? = nil
    
    /// The mode of the rate limiter, which can be either a leaky bucket or a token bucket.
    public enum Mode {
        /// A leaky bucket mode where tokens are regenerated at a specified rate.
        /// - Parameter tokenRate: The rate at which tokens are regenerated, expressed in tokens per second.
        case leakyBucket(tokenRate: Double)
        /// A token bucket mode where tokens are regenerated at a specified rate and can hold a maximum number of tokens.
        /// - Parameter maxTokens: The maximum number of tokens that can be held in the bucket.
        /// - Parameter tokenRate: The rate at which tokens are regenerated, expressed in tokens per second.
        case tokenBucket(maxTokens: Int, tokenRate: Double)
    }
    
    /// Initializes a new rate limiter with the specified mode.
    /// - Parameter mode: The mode of the rate limiter, which can be either a leaky bucket or a token bucket.
    public init(_ mode: Mode) {
        switch mode {
        case .leakyBucket(tokenRate: let tokenRate):
            self.maxTokens = 1
            self.tokenRate = tokenRate
        case .tokenBucket(maxTokens: let maxTokens, tokenRate: let tokenRate):
            self.maxTokens = maxTokens
            self.tokenRate = tokenRate
        }
        // Initialize the remaining tokens to the maximum number of tokens. The bucket starts full.
        self.tokensInBucket = maxTokens
        self.lastTokenRegenerate = .now
    }
    
    /// Regenerates tokens based on the elapsed time since the last regeneration.
    /// This method calculates how many tokens should be available based on the time elapsed and the token regeneration rate.
    /// If there are blocked waiters, it will resume them in the order they were blocked.
    /// It also updates the `lastTokenRegenerate` and `nextTokenRegenerate` timestamps accordingly.
    private func regenerateTokens() {
        let now = Date()
        let timePerToken = 1/tokenRate
        
        // Time since the last token regeneration
        let duration = now.timeIntervalSince(lastTokenRegenerate)
        // Calculate how many tokens should be available based on the elapsed time.
        let tokensInDuration = duration / timePerToken
        // Rounding down to the nearest whole number.
        var additionalTokensToRegenerate = Int(tokensInDuration)
        
        if additionalTokensToRegenerate > 0  {
            
            // While there are additional tokens to regenerate, we will resume blocked waiters.
            while additionalTokensToRegenerate > 0, let nextTicket = queue.popFirst() {
                blockedWaiters.removeValue(forKey: nextTicket)?.continuation?.resume()
                additionalTokensToRegenerate -= 1
            }
            
            // Update the number of tokens in the bucket, ensuring it does not exceed the maximum.
            tokensInBucket = min(maxTokens, tokensInBucket + additionalTokensToRegenerate)
            
            if tokensInBucket < maxTokens {
                // If the bucket is not full, we update the last regeneration time and schedule the next regeneration.

                // We set the lastTokenRegenerate time to the last time a full token was regenerated, therefore we subtract the duration 
                lastTokenRegenerate = now.addingTimeInterval(-duration + floor(tokensInDuration) * timePerToken)
                // The next token regeneration time is set to the last regeneration time plus the time per token.
                nextTokenRegenerate = lastTokenRegenerate.addingTimeInterval(timePerToken)
            } else {
                // If the bucket is full, we reset the last regeneration time and clear the next regeneration time.
                // This means we won't schedule any further regeneration until the next token is consumed.
                lastTokenRegenerate = now
                nextTokenRegenerate = nil
            }
        }
        
        // Start or stop the regeneration task based on the current state of the queue.
        updateRegenerationTask()
    }
    
    /// The loop that runs in the background to regenerate tokens at the specified rate. (see `regenerationTask`)
    /// It sleeps until the next token regeneration time and then regenerates tokens.
    private func executeScheduledRegeneration() async {
        do {
            var sleepDuration = 1/self.tokenRate
            if let nextTokenRegenerate = self.nextTokenRegenerate {
                sleepDuration = max(nextTokenRegenerate.timeIntervalSince(.now), 0)
            }
            try await Task.sleep(for: sleepDuration)
            self.regenerateTokens()
        } catch {}
    }
    
    /// Starts or stops the regeneration task based on the current state of the queue.
    /// If the queue is empty, the task is cancelled. If there are blocked waiters, a new task is created to handle token regeneration.
    /// This method ensures that the regeneration task is only running when necessary, saving resources when no tokens are needed.
    private func updateRegenerationTask() {
        if queue.isEmpty {
            regenerationTask?.cancel()
            regenerationTask = nil
        } else if regenerationTask == nil {
            regenerationTask = Task { [weak self] in
                while !Task.isCancelled, let self {
                    await self.executeScheduledRegeneration()
                }
            }
        }
    }
    
    /// Consumes a token from the rate limiter if available.
    /// - Returns `true` if a token was successfully consumed, `false` if no tokens are available.
    public func consumeToken() -> Bool {
        regenerateTokens()
        guard tokensInBucket > 0 else {
            return false
        }
        tokensInBucket -= 1
        return true
    }
    
    /// Attempts to consume a token from the rate limiter, throwing an error if no tokens are available.
    /// - Throws: `RateLimiterError.exceededRateLimit` if no tokens are available.
    public func tryConsumeToken() throws(RateLimiterError) {
        regenerateTokens()
        guard tokensInBucket > 0 else {
            throw RateLimiterError.exceededRateLimit
        }
        tokensInBucket -= 1
    }
    
    /// Handles cancellation of a blocked waiter.
    /// This method removes the waiter from the queue and cancels its continuation.
    /// - Parameter ticket: The ticket representing the blocked waiter to be cancelled.
    private func cancelBlockedWaiter(_ ticket: Ticket) {
        if let queueIndex = queue.firstIndex(of: ticket) {
            queue.remove(at: queueIndex)
        }
        blockedWaiters[ticket]?.cancel()
        blockedWaiters.removeValue(forKey: ticket)
    }
    
    /// Blocks the current task until a token is available, consuming it when it becomes available.
    /// If a token is already available, it is consumed and the method returns immediately.
    /// If no tokens are available, the task is blocked until a token is available, at which point it consumes the token and resumes.
    /// If multiple tasks are blocked, they will be resumed in the order they were blocked (FIFO).
    /// - Throws: `CancellationError` if the task is cancelled while waiting for a token.
    public func blockUntilNextTokenAvailable() async throws {
        try Task.checkCancellation()
        guard !consumeToken() else {
            return
        }
        
        let ticket = Ticket()
        blockedWaiters[ticket] = .init()
        
        return try await withTaskCancellationHandler {
            return try await withCheckedThrowingContinuation { continuation in
                // If the waiter has already been cancelled, we throw a CancellationError.
                guard blockedWaiters[ticket]?.isCancelled == false else {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                // Otherwise, we set the continuation for the blocked waiter and add it to the queue.
                // The continuation will be resumed when by the `regenerateTokens` method when a token becomes available.
                blockedWaiters[ticket]?.continuation = continuation
                queue.append(ticket)
                regenerateTokens()
            }
        } onCancel: {
            Task {
                await self.cancelBlockedWaiter(ticket)
            }
        }
    }
}


public enum RateLimiterError: Error, Sendable {
    case exceededRateLimit
}
