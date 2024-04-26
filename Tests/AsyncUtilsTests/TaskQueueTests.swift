//
//  TaskQueueTests.swift
//  
//
//  Created by Matteo Ludwig on 26.04.24.
//

import XCTest
@testable import AsyncUtils


final class TaskQueueTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    actor TestingStorage {
        var starts: [Int: Date] = [:]
        var ends: [Int: Date] = [:]
        
        var counter: Int = 0
        
        func incrementCounter() {
            self.counter += 1
        }
        
        func started(_ id: Int) {
            self.starts[id] = Date()
        }
        
        func ended(_ id: Int) {
            self.ends[id] = Date()
        }
    }
    
    func testAdd() async throws {
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 1)
        for _ in 0..<500 {
            await queue.add {
                try? await Task.sleep(for: .microseconds(1))
            }
        }
        let count1 = await queue.count
        let runningCount1 = await queue.runningCount
        let queuedCount1 = await queue.queuedCount
        XCTAssertGreaterThan(count1, 1)
        XCTAssertGreaterThan(queuedCount1, 1)
        XCTAssertEqual(runningCount1, 1)
        
        await queue.cancelAll()
    }
    
    func testAddDuplicate() async throws {
        let store = TestingStorage()
        let queue = TaskQueue<Int>(maxConcurrentTasks: 1)
        
        for _ in 0..<10 {
            await queue.add(with: 0) {
                try? await Task.sleep(for: .milliseconds(10))
                await store.incrementCounter()
            }
        }
        
        let counts1 = await queue.counts
        XCTAssertEqual(counts1, .init(queued: 0, running: 1))
        
        await queue.add(with: 1) {
            try? await Task.sleep(for: .milliseconds(10))
            await store.incrementCounter()
        }
        
        let counts2 = await queue.counts
        XCTAssertEqual(counts2, .init(queued: 1, running: 1))
        
        await queue.waitForAll()
        
        let counts3 = await queue.counts
        XCTAssertEqual(counts3, .init(queued: 0, running: 0))
        let counter3 = await store.counter
        XCTAssertEqual(counter3, 2)
        
        for _ in 0..<10 {
            await queue.add(with: 0) {
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
        
        let counts4 = await queue.counts
        XCTAssertEqual(counts4, .init(queued: 0, running: 1))
    }
    
    func testAddAndWait() async throws {
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 4)
        for _ in 0..<10 {
            await queue.add {
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
        let counts1 = await queue.counts
        XCTAssertGreaterThan(counts1.count, 1)
        XCTAssertGreaterThan(counts1.queued, 1)
        XCTAssertEqual(counts1.running, 4)
        
        await queue.addAndWait {
            try? await Task.sleep(for: .milliseconds(10))
        }
        
        let counts2 = await queue.counts
        XCTAssertEqual(counts2, .init(queued: 0, running: 0))
        
        await queue.add {
            try? await Task.sleep(for: .milliseconds(10))
        }
        
        let counts3 = await queue.counts
        XCTAssertEqual(counts3, .init(queued: 0, running: 1))
        
        await queue.cancelAll()
    }
    
    func testAddAndWaitDuplicates() async throws {
        let store = TestingStorage()
        let queue = TaskQueue<Int>(maxConcurrentTasks: 4)
        for _ in 0..<10 {
            await queue.add {
                try? await Task.sleep(for: .milliseconds(1))
            }
        }
        let counts1 = await queue.counts
        XCTAssertGreaterThan(counts1.count, 1)
        XCTAssertGreaterThan(counts1.queued, 1)
        XCTAssertEqual(counts1.running, 4)
        
        var tasks: [Task<Void, Never>] = []
        for i in 0..<9 {
            let task = Task {
                await queue.addAndWait(with: 0) {
                    await store.started(i)
                    try? await Task.sleep(for: .milliseconds(10))
                    await store.incrementCounter()
                    await store.ended(i)
                }
                
                let counter = await store.counter
                XCTAssertEqual(counter, 1)
                
                let counts = await queue.counts
                XCTAssertEqual(counts, .init(queued: 0, running: 0))
            }
            tasks.append(task)
        }
        await queue.addAndWait(with: 0) {
            await store.started(10)
            try? await Task.sleep(for: .milliseconds(10))
            await store.incrementCounter()
            await store.ended(10)
        }
        
        let ts = Date()
        
        let counts2 = await queue.counts
        XCTAssertEqual(counts2, .init(queued: 0, running: 0))
        
        let counter2 = await store.counter
        XCTAssertEqual(counter2, 1)
        
        let starts = await store.starts
        let ends = await store.ends
        
        XCTAssertEqual(starts.count, 1)
        XCTAssertEqual(ends.count, 1)
        XCTAssertEqual(starts.first?.key, 0)
        XCTAssertEqual(ends.first?.key, 0)
        
        XCTAssertLessThanOrEqual(ends[0]!, ts)
    
        for task in tasks {
            _ = await task.result
        }
    }
    
    
    func testWaitForAll() async throws {
        let store = TestingStorage()
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 20)
        for i in 0..<500 {
            await queue.add {
                try! await Task.sleep(for: .microseconds(1))
                await store.ended(i)
            }
        }
        
        await queue.waitForAll()
        let ts = Date()
        
        let count2 = await queue.count
        XCTAssertEqual(count2, 0)
        
        let lastTaskCompletion = await store.ends.map {$0.1}.max()!
        
        XCTAssertGreaterThan(ts, lastTaskCompletion)
        XCTAssertEqual(lastTaskCompletion.timeIntervalSinceReferenceDate, ts.timeIntervalSinceReferenceDate, accuracy: 0.001)
    }
    
    func testCancelQueued() async throws {
        let store = TestingStorage()
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 20)
        for i in 0..<500 {
            await queue.add {
                await store.started(i)
                await store.incrementCounter()
                do {
                    try await Task.sleep(for: .milliseconds(1))
                    await store.ended(i)
                } catch {}
            }
        }
        
        await queue.cancelQueued()
        await queue.waitForAll()
        
        let counter = await store.counter
        XCTAssertLessThan(counter, 500)
        
        let starts = await store.starts
        let ends = await store.ends
        
        XCTAssertEqual(starts.count, counter)
        XCTAssertEqual(ends.count, counter)
    }
    
    func testCancelAll() async throws {
        let store = TestingStorage()
        let cancellationStore = TestingStorage()
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 13)
        for i in 0..<500 {
            await queue.add {
                await store.started(i)
                await store.incrementCounter()
                do {
                    try await Task.sleep(for: .milliseconds(50))
                    await store.ended(i)
                } catch is CancellationError {
                    await cancellationStore.incrementCounter()
                } catch {
                    
                }
            }
        }
        
        try await Task.sleep(for: .milliseconds(70))
        await queue.cancelAllAndWait()
        
        let counter1 = await store.counter
        XCTAssertLessThan(counter1, 500)
        
        let cancellationCounter = await cancellationStore.counter
        XCTAssertGreaterThanOrEqual(cancellationCounter, 1)
        
        let starts = await store.starts
        let ends = await store.ends
        
        XCTAssertEqual(starts.count, counter1)
        XCTAssertLessThan(ends.count, counter1)
        
        for i in 0..<100 {
            await queue.add {
                await store.incrementCounter()
            }
        }
        await queue.waitForAll()
        let counter2 = await store.counter
        XCTAssertEqual(counter1 + 100, counter2)
    }
    
    func testOrderOfOperationsSerial() async throws {
        let store = TestingStorage()
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 1)
        for i in 0..<500 {
            await queue.add {
                await store.started(i)
                try! await Task.sleep(for: .microseconds(1))
                await store.ended(i)
            }
        }

        await queue.waitForAll()
     
        let starts = await store.starts
        let ends = await store.ends
        
        for i in 0..<500 {
            XCTAssertLessThanOrEqual(starts[i]!, ends[i]!)
        }
        
        for i in 1..<500 {
            XCTAssertLessThanOrEqual(ends[i-1]!, starts[i]!)
        }
    }
    
    func testOrderOfOperationsParallel() async throws {
        let store = TestingStorage()
        let queue = AnonymousTaskQueue(maxConcurrentTasks: 3)
        for i in 0...7 {
            await queue.add {
                await store.started(i)
                try! await Task.sleep(for: .microseconds(1000*i))
                await store.ended(i)
            }
        }

        await queue.waitForAll()
     
        let starts = await store.starts
        let ends = await store.ends
        
        for i in 0...6 {
            XCTAssertLessThanOrEqual(starts[i]!, ends[i]!)
        }
        
     
        XCTAssertLessThanOrEqual(starts[0]!, starts[3]!)
        XCTAssertLessThanOrEqual(starts[1]!, starts[3]!)
        XCTAssertLessThanOrEqual(starts[2]!, starts[3]!)
        
        XCTAssertLessThanOrEqual(ends[0]!, starts[3]!)
        XCTAssertLessThanOrEqual(ends[1]!, starts[4]!)
        XCTAssertLessThanOrEqual(ends[2]!, starts[5]!)
        XCTAssertLessThanOrEqual(ends[3]!, starts[6]!)
        XCTAssertLessThanOrEqual(ends[4]!, starts[7]!)
    }

}
