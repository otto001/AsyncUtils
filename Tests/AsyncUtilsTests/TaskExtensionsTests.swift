//
//  TaskExtensionsTests.swift
//  
//
//  Created by Matteo Ludwig on 06.05.24.
//

import XCTest

final class TaskExtensionsTests: XCTestCase {

    var store = TestingStorage()
    
    override func setUpWithError() throws {
        self.store = .init()
    }

    func testSleepFor() async throws {
        let start = Date()
        try await Task.sleep(for: 0.1)
        let delta = Date().timeIntervalSince(start)
        XCTAssertEqual(delta, 0.1, accuracy: 0.01)
    }
    
    func testDelayedTask() async throws {
        let start = Date()
        let task = Task.delayed(by: 0.1) {
            let delta = Date().timeIntervalSince(start)
            XCTAssertEqual(delta, 0.1, accuracy: 0.01)
        }
        try await task.value
        let delta = Date().timeIntervalSince(start)
        XCTAssertEqual(delta, 0.1, accuracy: 0.01)
    }
    
    func testTaskWithTimeout() async throws {
        let start = Date()
        let task = Task.withTimeout(cancelAfter: 0.1) {
            try await Task.sleep(for: 0.06)
            XCTAssertFalse(Task.isCancelled)
            try? await Task.sleep(for: 0.06)
            XCTAssertTrue(Task.isCancelled)
        }
        try await task.value
    }
}
