//
//  RateLimiterTests.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 21.05.25.
//

import XCTest
@testable import AsyncUtils

final class RateLimiterTests: XCTestCase {


    func testLeakyBucketRegenerate() async throws {
        let rateLimiter = RateLimiter(.leakyBucket(averageRate: 10))
        let consumeFirst = await rateLimiter.consumeToken()
        let firstConsumed = Date()
        XCTAssertTrue(consumeFirst)
        
        while Date().timeIntervalSince(firstConsumed) < 0.1 {
            let currentTokens = await rateLimiter.currentTokens
            XCTAssertEqual(currentTokens, 0)
            try await Task.sleep(for: .milliseconds(10))
            
        }
        let currentTokens = await rateLimiter.currentTokens
        XCTAssertEqual(currentTokens, 1)
    }

    func testLeakyBucketBlocking() async throws {
        let rateLimiter = RateLimiter(.leakyBucket(averageRate: 100))
        let storage = TestingStorage()
        
        let count = 300
        for i in 0..<count {
            Task.detached {
                await storage.started(i)
                try await rateLimiter.blockUntilNextTokenAvailable()
                await storage.ended(i)
            }
        }
        
        try await Task.sleep(for: 3.5)
        let (_, ends, _) = await storage.data
        XCTAssertEqual(ends.count, count)
        
        let sortedEnds = ends.values.sorted()
        
        let deltaTime = sortedEnds.last!.timeIntervalSince(sortedEnds.first!)
        XCTAssertEqual(Double(count)/deltaTime, 100.0, accuracy: 0.5)
        
        for i in 1..<count {
            XCTAssertEqual(sortedEnds[i].timeIntervalSinceReferenceDate - sortedEnds[i-1].timeIntervalSinceReferenceDate,
                           0.01, accuracy: 0.003)
        }
    }

}
