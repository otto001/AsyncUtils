//
//  AsyncSemaphoreTests.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 21.05.25.
//

// Tests originally from <https://github.com/groue/Semaphore/pull/3>

import Dispatch
import XCTest
@testable import AsyncUtils

final class AsyncSemaphoreTests: XCTestCase {
    
    func testSignalWithoutSuspendedTasks() async {
        // Check DispatchSemaphore behavior
        do {
            do {
                let sem = DispatchSemaphore(value: 0)
                XCTAssertFalse(sem.signal() != 0)
            }
            do {
                let sem = DispatchSemaphore(value: 1)
                XCTAssertFalse(sem.signal() != 0)
            }
            do {
                let sem = DispatchSemaphore(value: 2)
                XCTAssertFalse(sem.signal() != 0)
            }
        }
        
        // Test that AsyncSemaphore behaves identically
        do {
            do {
                let sem = AsyncSemaphore(value: 0)
                let woken = await sem.signal()
                XCTAssertFalse(woken)
            }
            do {
                let sem = AsyncSemaphore(value: 1)
                let woken = await sem.signal()
                XCTAssertFalse(woken)
            }
            do {
                let sem = AsyncSemaphore(value: 2)
                let woken = await sem.signal()
                XCTAssertFalse(woken)
            }
        }
    }
    
    func testSimpleResumeOnSignal() async throws {
        // Check DispatchSemaphore behavior
        do {
            // Given a thread waiting for the semaphore
            let sem = DispatchSemaphore(value: 0)
            Thread { sem.wait() }.start()
            try await Task.sleep(for: 0.05)
            
            // First signal wakes the waiting thread
            XCTAssertTrue(sem.signal() != 0)
            // Second signal does not wake any thread
            XCTAssertFalse(sem.signal() != 0)
        }
        
        // Test that AsyncSemaphore behaves identically
        do {
            // Given a task suspended on the semaphore
            let sem = AsyncSemaphore(value: 0)
            Task { try await sem.wait() }
            try await Task.sleep(for: 0.05)
            
            
            let firstSignal = await sem.signal()
            let secondSignal = await sem.signal()
            // First signal resumes the suspended task
            XCTAssertTrue(firstSignal)
            // Second signal does not resume any task
            XCTAssertFalse(secondSignal)
        }
    }
    
    func testWaitBeforeSignal() async {
        // Check DispatchSemaphore behavior
        do {
            // Given a zero semaphore
            let sem = DispatchSemaphore(value: 0)
            
            // When a thread waits for this semaphore,
            let ex1 = expectation(description: "wait")
            ex1.isInverted = true
            let ex2 = expectation(description: "woken")
            Thread {
                sem.wait()
                ex1.fulfill()
                ex2.fulfill()
            }.start()
            
            // Then the thread is initially blocked.
            await fulfillment(of: [ex1], timeout: 0.5)
            
            // When a signal occurs, then the waiting thread is woken.
            sem.signal()
            await fulfillment(of: [ex2], timeout: 1)
        }
        
        // Test that AsyncSemaphore behaves identically
        do {
            // Given a zero semaphore
            let sem = AsyncSemaphore(value: 0)
            
            // When a task waits for this semaphore,
            let ex1 = expectation(description: "wait")
            ex1.isInverted = true
            let ex2 = expectation(description: "woken")
            Task {
                do {
                    try await sem.wait()
                    ex1.fulfill()
                    ex2.fulfill()
                } catch {
                    XCTFail("Unexpected error")
                }
            }
            
            // Then the task is initially suspended.
            await fulfillment(of: [ex1], timeout: 0.5)
            
            // When a signal occurs, then the suspended task is resumed.
            await sem.signal()
            await fulfillment(of: [ex2], timeout: 0.5)
        }
    }
    
    func testCancellationDuringWaitThrowsCancellationError() async throws {
        let sem = AsyncSemaphore(value: 0)
        let ex = expectation(description: "cancellation")
        let task = Task {
            do {
                try await sem.wait()
                XCTFail("Expected CancellationError")
            } catch is CancellationError {
            } catch {
                XCTFail("Unexpected error")
            }
            ex.fulfill()
        }
        try await Task.sleep(for: 0.1)
        task.cancel()
        await fulfillment(of: [ex], timeout: 1)
    }
    
    func testCancellationBeforeWaitThrowsCancellationError() throws {
        let sem = AsyncSemaphore(value: 0)
        let ex = expectation(description: "cancellation")
        let task = Task {
            // Uncancellable delay
            await withUnsafeContinuation { continuation in
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                    continuation.resume()
                }
            }
            do {
                try await sem.wait()
                XCTFail("Expected CancellationError")
            } catch is CancellationError {
            } catch {
                XCTFail("Unexpected error")
            }
            ex.fulfill()
        }
        task.cancel()
        wait(for: [ex], timeout: 5)
    }
    
    func testCancellationDuringWaitReIncrementsValue() async throws {
        // Given a task cancelled while suspended on a semaphore,
        let sem = AsyncSemaphore(value: 0)
        let task = Task {
            try await sem.wait()
        }
        try await Task.sleep(for: 0.1)
        task.cancel()
        
        // When a task waits for this semaphore,
        let ex1 = expectation(description: "wait")
        ex1.isInverted = true
        let ex2 = expectation(description: "woken")
        Task {
            do {
                try await sem.wait()
                ex1.fulfill()
                ex2.fulfill()
            } catch {
                XCTFail("Unexpected error")
            }
        }
        
        // Then the task is initially suspended.
        await fulfillment(of: [ex1], timeout: 0.5)
        
        // When a signal occurs, then the suspended task is resumed.
        await sem.signal()
        await fulfillment(of: [ex2], timeout: 0.5)
    }
    
    func testCancellationBeforeWaitReIncrementsValue() async throws {
        // Given a task cancelled before it waits on a semaphore,
        let sem = AsyncSemaphore(value: 0)
        let task = Task {
            // Uncancellable delay
            await withUnsafeContinuation { continuation in
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                    continuation.resume()
                }
            }
            try await sem.wait()
        }
        task.cancel()
        
        // When a task waits for this semaphore,
        let ex1 = expectation(description: "wait")
        ex1.isInverted = true
        let ex2 = expectation(description: "woken")
        Task {
            do {
                try await sem.wait()
                ex1.fulfill()
                ex2.fulfill()
            } catch {
                XCTFail("Unexpected error")
            }
        }
        
        // Then the task is initially suspended.
        await fulfillment(of: [ex1], timeout: 0.5)
        
        // When a signal occurs, then the suspended task is resumed.
        await sem.signal()
        await fulfillment(of: [ex2], timeout: 0.5)
    }
    
    // Inspired by <https://github.com/groue/Semaphore/pull/3>
    func testCancellationBeforeWaitDoesNotDecrement() async {
        // Given a task that waits for a semaphore with value 1 after the
        // task has been cancelled,
        let sem = AsyncSemaphore(value: 1)
        let task = Task {
            while !Task.isCancelled {
                await Task.yield()
            }
            try await sem.wait()
        }
        task.cancel()
        try? await task.value
        
        // When a second task waits for this semaphore,
        let ex = expectation(description: "wait")
        Task {
            do {
                try await sem.wait()
                ex.fulfill()
            } catch {
                XCTFail("Unexpected error")
            }
        }
        
        // Then the second task is not suspended.
        await fulfillment(of: [ex], timeout: 0.5)
    }
    
    // Test that semaphore can limit the number of concurrent executions of
    // an actor method.
    func testResourceLimitingForActor() async {
        /// An actor that limits the number of concurrent executions of
        /// its `run()` method, and counts the effective number of
        /// concurrent executions for testing purpose.
        actor Runner {
            private let semaphore: AsyncSemaphore
            private var count = 0
            private(set) var effectiveMaxConcurrentRuns = 0
            
            init(maxConcurrentRuns: Int) {
                semaphore = AsyncSemaphore(value: maxConcurrentRuns)
            }
            
            func run() async {
                do {
                    try await semaphore.wait()
                } catch {
                    XCTFail("Unexpected error")
                }
                
                count += 1
                effectiveMaxConcurrentRuns = max(effectiveMaxConcurrentRuns, count)
                try! await Task.sleep(for: 0.01)
                count -= 1
                await semaphore.signal()
            }
        }
        
        for maxConcurrentRuns in 1...10 {
            let runner = Runner(maxConcurrentRuns: maxConcurrentRuns)
            
            // Spawn many concurrent tasks
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<50 {
                    group.addTask {
                        await runner.run()
                    }
                }
            }
            
            let effectiveMaxConcurrentRuns = await runner.effectiveMaxConcurrentRuns
            XCTAssertEqual(effectiveMaxConcurrentRuns, maxConcurrentRuns)
        }
    }
    
    // Test that semaphore can limit the number of concurrent executions of
    // an async method.
    func testResourceLimitingForAsyncMethod() async {
        /// A class that limits the number of concurrent executions of
        /// its `run()` method, and counts the effective number of
        /// concurrent executions for testing purpose.
        @MainActor
        class Runner {
            private let semaphore: AsyncSemaphore
            private var count = 0
            private(set) var effectiveMaxConcurrentRuns = 0
            
            init(maxConcurrentRuns: Int) {
                semaphore = AsyncSemaphore(value: maxConcurrentRuns)
            }
            
            func run() async {
                do {
                    try await semaphore.wait()
                } catch {
                    XCTFail("Unexpected error")
                }
                
                count += 1
                effectiveMaxConcurrentRuns = max(effectiveMaxConcurrentRuns, count)
                try! await Task.sleep(for: 0.01)
                count -= 1
                await semaphore.signal()
            }
        }
        
        for maxConcurrentRuns in 1...10 {
            let runner = await Runner(maxConcurrentRuns: maxConcurrentRuns)
            
            // Spawn many concurrent tasks
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<50 {
                    group.addTask {
                        await runner.run()
                    }
                }
            }
            
            let effectiveMaxConcurrentRuns = await runner.effectiveMaxConcurrentRuns
            XCTAssertEqual(effectiveMaxConcurrentRuns, maxConcurrentRuns)
        }
    }
    
    // Test that semaphore can limit the number of concurrent executions of
    // an async method, even when interactions with Swift concurrency runtime
    // are (as much as possible) initiated from a single thread.
    func testResourceLimitingOnSingleTask() async {
        /// A class that limits the number of concurrent executions of
        /// its `run()` method, and counts the effective number of
        /// concurrent executions for testing purpose.
        @MainActor
        class Runner {
            private let semaphore: AsyncSemaphore
            private var count = 0
            private(set) var effectiveMaxConcurrentRuns = 0
            
            init(maxConcurrentRuns: Int) {
                semaphore = AsyncSemaphore(value: maxConcurrentRuns)
            }
            
            func run() async {
                do {
                    try await semaphore.wait()
                } catch {
                    XCTFail("Unexpected error")
                }
                
                count += 1
                effectiveMaxConcurrentRuns = max(effectiveMaxConcurrentRuns, count)
                try! await Task.sleep(nanoseconds: 100_000_000)
                count -= 1
                
                await semaphore.signal()
            }
        }
        
        await Task { @MainActor in
            let runner = Runner(maxConcurrentRuns: 3)
            async let x0: Void = runner.run()
            async let x1: Void = runner.run()
            async let x2: Void = runner.run()
            async let x3: Void = runner.run()
            async let x4: Void = runner.run()
            async let x5: Void = runner.run()
            async let x6: Void = runner.run()
            async let x7: Void = runner.run()
            async let x8: Void = runner.run()
            async let x9: Void = runner.run()
            _ = await (x0, x1, x2, x3, x4, x5, x6, x7, x8, x9)
            
            let effectiveMaxConcurrentRuns = runner.effectiveMaxConcurrentRuns
            XCTAssertEqual(effectiveMaxConcurrentRuns, 3)
            
        }.value
    }
    
    func testResourceLimitingOnRun() async throws {
        /// A class that limits the number of concurrent executions of
        /// its `run()` method, and counts the effective number of
        /// concurrent executions for testing purpose.
        @MainActor
        class Runner {
            private let semaphore: AsyncSemaphore
            private var count = 0
            private(set) var effectiveMaxConcurrentRuns = 0
            
            init(maxConcurrentRuns: Int) {
                semaphore = AsyncSemaphore(value: maxConcurrentRuns)
            }
            
            func run() async {
                do {
                    try await semaphore.run {
                        
                        count += 1
                        effectiveMaxConcurrentRuns = max(effectiveMaxConcurrentRuns, count)
                        try! await Task.sleep(for: 0.01)
                        count -= 1
                    }
                } catch {
                    XCTFail("Unexpected error")
                }
            }
        }
        
        for maxConcurrentRuns in 1...10 {
            let runner = await Runner(maxConcurrentRuns: maxConcurrentRuns)
            
            // Spawn many concurrent tasks
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<50 {
                    group.addTask {
                        await runner.run()
                    }
                }
            }
            
            let effectiveMaxConcurrentRuns = await runner.effectiveMaxConcurrentRuns
            XCTAssertEqual(effectiveMaxConcurrentRuns, maxConcurrentRuns)
        }
    }
}
