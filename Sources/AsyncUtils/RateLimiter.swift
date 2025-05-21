//
//  RateLimiter.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 21.05.25.
//

import Foundation
import DequeModule

actor RateLimiter {
    var maxTokens: Int
    var averageRate: Double
    
    private var remainingTokens: Int
    private var lastTokenRegenerate: Date
    private var nextTokenRegenerate: Date?
    
    private var lastSendTime: Date = .distantPast
    
    var currentTokens: Int {
        regenerateTokens()
        return remainingTokens
    }
    
    private(set) var blocks: Deque<CheckedContinuation<Void, Error>> = []
    private(set) var regenerationTask: Task<Void, Never>? = nil
    
    enum Mode {
        case leakyBucket(averageRate: Double)
        case tokenBucket(maxTokens: Int, averageRate: Double)
    }
    
    init(_ mode: Mode) {
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
                nextBlocking.resume()
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
    
    func consumeToken() -> Bool {
        regenerateTokens()
        guard remainingTokens > 0 else {
            return false
        }
        remainingTokens -= 1
        lastSendTime = .now
        return true
    }
    
    func tryConsumeToken() throws {
        regenerateTokens()
        guard remainingTokens > 0 else {
            throw RateLimiterError.exceededRateLimit
        }
        remainingTokens -= 1
    }
    
    func blockUntilNextTokenAvailable() async throws {
        guard !consumeToken() else {
            return
        }
        
        return try await withCheckedThrowingContinuation { continuation in
            blocks.append(continuation)
            regenerateTokens()
        }
    }
}


enum RateLimiterError: Error {
    case exceededRateLimit
}
