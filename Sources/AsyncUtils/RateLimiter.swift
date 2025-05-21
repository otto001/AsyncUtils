//
//  RateLimiter.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 21.05.25.
//

import Foundation
import DequeModule

public actor RateLimiter {
    public var maxTokens: Int
    public var averageRate: Double
    
    private var remainingTokens: Int
    private var lastTokenRegenerate: Date
    private var nextTokenRegenerate: Date?
    
    private var lastSendTime: Date = .distantPast
    
    public var currentTokens: Int {
        regenerateTokens()
        return remainingTokens
    }
    
    private final class BlockedWaiter: @unchecked Sendable, Identifiable {
        var continuation: CheckedContinuation<Void, Error>?
        
        init(continuation: CheckedContinuation<Void, Error>? = nil) {
            self.continuation = continuation
        }
    }
    
    private var blocks: Deque<BlockedWaiter> = []
    private var regenerationTask: Task<Void, Never>? = nil
    
    public enum Mode {
        case leakyBucket(averageRate: Double)
        case tokenBucket(maxTokens: Int, averageRate: Double)
    }
    
    public init(_ mode: Mode) {
        switch mode {
        case .leakyBucket(averageRate: let averageRate):
            self.maxTokens = 1
            self.averageRate = averageRate
        case .tokenBucket(maxTokens: let maxTokens, averageRate: let averageRate):
            self.maxTokens = maxTokens
            self.averageRate = averageRate
        }
        self.remainingTokens = maxTokens
        self.lastTokenRegenerate = .now
    }
    
    private func regenerateTokens() {
        let now = Date()
        let timePerToken = 1/averageRate
        
        let duration = now.timeIntervalSince(lastTokenRegenerate)
        let tokensInDuration = duration / timePerToken
        var additionalTokensToRegenerate = Int(tokensInDuration)
        
        if additionalTokensToRegenerate > 0  {
            
            while additionalTokensToRegenerate > 0, let nextBlocking = blocks.popFirst() {
                nextBlocking.continuation?.resume()
                additionalTokensToRegenerate -= 1
            }
            
            remainingTokens = min(maxTokens, remainingTokens + additionalTokensToRegenerate)
            
            if remainingTokens < maxTokens {
                lastTokenRegenerate = now.addingTimeInterval(-duration + floor(tokensInDuration) * timePerToken)
                nextTokenRegenerate = lastTokenRegenerate.addingTimeInterval(timePerToken)
            } else {
                lastTokenRegenerate = now
                nextTokenRegenerate = nil
            }
        }
        
        updateRegenerationTask()
    }
    
    private func executeScheduledRegeneration() async {
        do {
            var sleepDuration = 1/self.averageRate
            if let nextTokenRegenerate = self.nextTokenRegenerate {
                sleepDuration = max(nextTokenRegenerate.timeIntervalSince(.now), 0)
            }
            try await Task.sleep(for: sleepDuration)
            self.regenerateTokens()
        } catch {}
    }
    
    private func updateRegenerationTask() {
        if blocks.isEmpty {
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
    
    public func consumeToken() -> Bool {
        regenerateTokens()
        guard remainingTokens > 0 else {
            return false
        }
        remainingTokens -= 1
        lastSendTime = .now
        return true
    }
    
    public func tryConsumeToken() throws {
        regenerateTokens()
        guard remainingTokens > 0 else {
            throw RateLimiterError.exceededRateLimit
        }
        remainingTokens -= 1
    }
    
    private func cancelBlockedWaiter(_ blockedWaiter: BlockedWaiter) {
        if let index = blocks.firstIndex(where: {$0 === blockedWaiter}) {
            blocks.remove(at: index)
            blockedWaiter.continuation?.resume(throwing: CancellationError())
        }
    }
    
    public func blockUntilNextTokenAvailable() async throws {
        guard !consumeToken() else {
            return
        }
        
        let blockedWaiter = BlockedWaiter()
        
        return try await withTaskCancellationHandler {
            return try await withCheckedThrowingContinuation { continuation in
                #if DEBUG
                self.assertIsolated()
                #endif
                blockedWaiter.continuation = continuation
                blocks.append(blockedWaiter)
                regenerateTokens()
            }
        } onCancel: {
            Task {
                await self.cancelBlockedWaiter(blockedWaiter)
            }
        }
    }
}


public enum RateLimiterError: Error, Sendable {
    case exceededRateLimit
}
