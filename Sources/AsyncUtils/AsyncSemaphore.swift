//
//  AsyncSemaphore.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 05.06.25.
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

/// A Semaphore that allows asynchronous waiting and signaling, mimicking the behavior of a DispatchSemaphore.
/// It supports cancellation of waiting tasks. 
/// Waiting tasks are signaled in FIFO order in regards to when they were blocked.
/// -Note: Using `.run` allows you to run an action while holding the semaphore, automatically signaling it after the action completes or if an error occurs.
public actor AsyncSemaphore {
    
    /// Internal type to represent a task waiting for the semaphore.
    internal struct BlockedWaiter: Sendable {
        /// Flag to indicate if the waiter has been cancelled.
        private(set) var isCancelled: Bool = false
        /// Continuation to resume the waiting task when the semaphore is signaled.
        /// This is set after the continuation is created.
        var continuation: CheckedContinuation<Void, Error>?
        
        init(continuation: CheckedContinuation<Void, Error>? = nil) {
            self.continuation = continuation
        }
        
        /// Cancels the waiter, resuming the continuation with a CancellationError if it exists.
        /// This method ensures that the waiter can only be cancelled once.
        mutating func cancel() {
            // ensure that we only cancel once
            guard !isCancelled else { return }
            isCancelled = true
            continuation?.resume(throwing: CancellationError())
        }
    }
    
    /// Current value of the semaphore, representing the number of available permits.
    /// - Important: In almost all cases, you should not to access this value, as doing so will only lead to race conditions.
    /// Use `wait()` to wait for a permit and `signal()` to release one.
    public private(set) var value: Int = 0

    /// All current blocked waiters, indexed by their ticket.
    /// This dictionary maps each `Ticket` to its corresponding `BlockedWaiter`.
    /// - Note: This is used to manage the waiters and their continuations.
    /// It allows for cancellation of waiters and resuming their continuations when a permit is signaled.
    private var blockedWaiters: [Ticket: BlockedWaiter] = [:]

    /// A queue of tickets representing the blocked waiters. The queue is used to manage the order in which waiters are signaled when a permit becomes available.
    private var queue: Deque<Ticket> = []
    
    public init(value: Int) {
        self.value = value
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
    
    /// Waits for a permit from the semaphore.
    /// If a permit is available, it decrements the value and returns immediately.
    /// If no permits are available, it blocks the current task until a permit is signaled.
    /// If the task is cancelled while waiting, a `CancellationError` is thrown.
    /// - Throws: `CancellationError` if the task is cancelled while waiting.
    /// - Note: If the task calling this method is already cancelled at the moment of calling this method, the semaphore will not be waited on, and the method will throw a `CancellationError`.
    public func wait() async throws {
        try Task.checkCancellation()
        
        guard value == 0 else {
            value -= 1
            return
        }
        
        let ticket = Ticket()
        blockedWaiters[ticket] = .init()
        
        return try await withTaskCancellationHandler {
            return try await withCheckedThrowingContinuation { continuation in
                guard blockedWaiters[ticket]?.isCancelled == false else {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                blockedWaiters[ticket]?.continuation = continuation
                queue.append(ticket)
            }
        } onCancel: {
            Task {
                await self.cancelBlockedWaiter(ticket)
            }
        }
    }
    
    /// Signals the semaphore, incrementing the value and waking up one waiting task if any are blocked.
    /// If there are no waiting tasks, it simply increments the value.
    /// - Returns: `true` if a waiting task was signaled, `false` if there were no waiting tasks.
    @discardableResult
    public func signal() -> Bool {
        value += 1
        guard let firstTicket = queue.popFirst() else {
            return false
        }
        guard let waiter = blockedWaiters[firstTicket] else {
            preconditionFailure("Missing waiter for ticket \(firstTicket)")
        }
        blockedWaiters.removeValue(forKey: firstTicket)
        guard !waiter.isCancelled else {
            preconditionFailure("Cancelled waiter for ticket \(firstTicket) should not have been in the queue")
        }
        waiter.continuation?.resume(returning: ())
        return true
    }
    
    /// Runs an asynchronous action while holding the semaphore.
    /// This method waits for a permit, executes the action, and signals the semaphore after the action completes or if an error occurs.
    /// - Parameter action: The asynchronous action to run while holding the semaphore.
    /// - Returns: The result of the action.
    /// - Throws: Any error thrown by the action or a `CancellationError` if the task is cancelled while waiting.
    public func run<T>(_ action: () async throws -> T) async throws -> T {
        try await self.wait()
       
        do {
            let result = try await action()
            self.signal()
            return result
        } catch {
            self.signal()
            throw error
        }
    }
}
