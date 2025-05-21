//
//  TaskQueueTests.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 24.04.24.
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

import XCTest
@testable import AsyncUtils


final class TaskQueueTests: XCTestCase {
    var queue: TaskQueue<Int> = .init()
    var store: TestingStorage = .init()
    
    override func setUpWithError() throws {
        self.store = TestingStorage()
        self.queue = .init(maxConcurrentTasks: 3)
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    
    func testAdd() async throws {
        self.queue = .init(maxConcurrentTasks: 1)
        for _ in 0..<500 {
            await self.queue.add {
                try? await Task.sleep(for: .microseconds(1))
            }
        }
        let count1 = await self.queue.count
        let runningCount1 = await self.queue.runningCount
        let queuedCount1 = await self.queue.queuedCount
        XCTAssertGreaterThan(count1, 1)
        XCTAssertGreaterThan(queuedCount1, 1)
        XCTAssertEqual(runningCount1, 1)
        
        await self.queue.cancelAll()
    }
    
    func testAddDuplicate() async throws {
        
        self.queue = .init(maxConcurrentTasks: 1)
        
        for _ in 0..<10 {
            await self.queue.add(with: 0) {
                try? await Task.sleep(for: .milliseconds(10))
                await self.store.incrementCounter()
            }
        }
        
        let counts1 = await self.queue.counts
        XCTAssertEqual(counts1, .init(queued: 0, running: 1))
        
        await self.queue.add(with: 1) {
            try? await Task.sleep(for: .milliseconds(10))
            await self.store.incrementCounter()
        }
        
        let counts2 = await self.queue.counts
        XCTAssertEqual(counts2, .init(queued: 1, running: 1))
        
        try! await self.queue.waitForAll()
        
        let counts3 = await self.queue.counts
        XCTAssertEqual(counts3, .init(queued: 0, running: 0))
        let counter3 = await self.store.counter
        XCTAssertEqual(counter3, 2)
        
        for _ in 0..<10 {
            await self.queue.add(with: 0) {
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
        
        let counts4 = await self.queue.counts
        XCTAssertEqual(counts4, .init(queued: 0, running: 1))
    }
    
    func testAddAndWait() async throws {
        for _ in 0..<10 {
            await self.queue.add {
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
        let counts1 = await self.queue.counts
        XCTAssertGreaterThan(counts1.count, 1)
        XCTAssertGreaterThan(counts1.queued, 1)
        XCTAssertEqual(counts1.running, 3)
        
        try await self.queue.addAndWait {
            try? await Task.sleep(for: .milliseconds(10))
        }
        
        let counts2 = await self.queue.counts
        XCTAssertEqual(counts2, .init(queued: 0, running: 0))
        
        await self.queue.add {
            try? await Task.sleep(for: .milliseconds(10))
        }
        
        let counts3 = await self.queue.counts
        XCTAssertEqual(counts3, .init(queued: 0, running: 1))
        
        await self.queue.cancelAll()
    }
    
    func testAddAndWaitDuplicates() async throws {
        for _ in 0..<10 {
            await self.queue.add {
                try? await Task.sleep(for: .milliseconds(1))
            }
        }
        let counts1 = await self.queue.counts
        XCTAssertGreaterThan(counts1.count, 1)
        XCTAssertGreaterThan(counts1.queued, 1)
        XCTAssertEqual(counts1.running, 3)
        
        var tasks: [Task<Void, Never>] = []
        for i in 0..<9 {
            let task = Task {
                try! await self.queue.addAndWait(with: 0) {
                    await self.store.started(i)
                    try? await Task.sleep(for: .milliseconds(10))
                    await self.store.incrementCounter()
                    await self.store.ended(i)
                }
                
                let counter = await self.store.counter
                XCTAssertEqual(counter, 1)
                
                let counts = await self.queue.counts
                XCTAssertEqual(counts, .init(queued: 0, running: 0))
            }
            tasks.append(task)
        }
        try! await self.queue.addAndWait(with: 0) {
            await self.store.started(10)
            try? await Task.sleep(for: .milliseconds(10))
            await self.store.incrementCounter()
            await self.store.ended(10)
        }
        
        let ts = Date()
        
        let counts2 = await self.queue.counts
        XCTAssertEqual(counts2, .init(queued: 0, running: 0))
        
        let counter2 = await self.store.counter
        XCTAssertEqual(counter2, 1)
        
        let starts = await self.store.starts
        let ends = await self.store.ends
        
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
        for i in 0..<100 {
            await self.queue.add {
                try! await Task.sleep(for: .milliseconds(1))
                await self.store.ended(i)
            }
        }
        
        try! await self.queue.waitForAll()
        let ts = Date()
        
        let count2 = await self.queue.count
        XCTAssertEqual(count2, 0)
        
        let lastTaskCompletion = await self.store.ends.map {$0.1}.max()!
        
        XCTAssertGreaterThan(ts, lastTaskCompletion)
        XCTAssertEqual(lastTaskCompletion.timeIntervalSinceReferenceDate, ts.timeIntervalSinceReferenceDate, accuracy: 0.001)
    }
    
    func testWaitForAllCancellation() async throws {
        self.queue = .init(maxConcurrentTasks: 2)
        await self.queue.add {
            try? await Task.sleep(for: 0.1)
        }
        await self.queue.add {
            try? await Task.sleep(for: 1.0)
        }
        
        
        let start1 = Date()
        // Try to cancel waitForAll while the queue has no free running slots (2 tasks running)
        try await Task.withTimeout(cancelAfter: 0.05) {
            try? await self.queue.waitForAll()
        }
        let delta1 = Date().timeIntervalSince(start1)
        XCTAssertEqual(delta1, 0.05, accuracy: 0.01)
        
        try? await Task.sleep(for: 0.06)
        
        let start2 = Date()
        // Try to cancel waitForAll while the queue has free running slots (1 tasks running)
        try await Task.withTimeout(cancelAfter: 0.1) {
            try? await self.queue.waitForAll()
        }
        let delta2 = Date().timeIntervalSince(start2)
        XCTAssertEqual(delta2, 0.1, accuracy: 0.1)

        try await queue.cancelAllAndWait()
    }
    
    func testCancelQueued() async throws {
        for i in 0..<500 {
            await self.queue.add {
                await self.store.started(i)
                await self.store.incrementCounter()
                do {
                    try await Task.sleep(for: .milliseconds(1))
                    await self.store.ended(i)
                } catch {}
            }
        }
        
        await self.queue.cancelQueued()
        try! await self.queue.waitForAll()
        
        let counter = await self.store.counter
        XCTAssertLessThan(counter, 500)
        
        let starts = await self.store.starts
        let ends = await self.store.ends
        
        XCTAssertEqual(starts.count, counter)
        XCTAssertEqual(ends.count, counter)
    }
    
    func testCancelAll() async throws {
        let cancellationStore = TestingStorage()
        self.queue = .init(maxConcurrentTasks: 13)
        for i in 0..<500 {
            await self.queue.add {
                await self.store.started(i)
                await self.store.incrementCounter()
                do {
                    try await Task.sleep(for: .milliseconds(50))
                    await self.store.ended(i)
                } catch is CancellationError {
                    await cancellationStore.incrementCounter()
                } catch {
                    
                }
            }
        }
        
        try await Task.sleep(for: .milliseconds(70))
        try! await self.queue.cancelAllAndWait()
        
        let counter1 = await self.store.counter
        XCTAssertLessThan(counter1, 500)
        
        let cancellationCounter = await cancellationStore.counter
        XCTAssertGreaterThanOrEqual(cancellationCounter, 1)
        
        let starts = await self.store.starts
        let ends = await self.store.ends
        
        XCTAssertEqual(starts.count, counter1)
        XCTAssertLessThan(ends.count, counter1)
        
        // Check that the queue still works
        for _ in 0..<100 {
            await self.queue.add {
                await self.store.incrementCounter()
            }
        }
        try! await self.queue.waitForAll()
        let counter2 = await self.store.counter
        XCTAssertEqual(counter1 + 100, counter2)
    }
    
    func testOrderOfOperationsSerial() async throws {
        self.queue = .init(maxConcurrentTasks: 1)
        for i in 0..<500 {
            await self.queue.add {
                await self.store.started(i)
                try! await Task.sleep(for: .microseconds(1))
                await self.store.ended(i)
            }
        }

        try! await self.queue.waitForAll()
     
        let starts = await self.store.starts
        let ends = await self.store.ends
        
        for i in 0..<500 {
            XCTAssertLessThanOrEqual(starts[i]!, ends[i]!)
        }
        
        for i in 1..<500 {
            XCTAssertLessThanOrEqual(ends[i-1]!, starts[i]!)
        }
    }
    
    func testOrderOfOperationsParallel() async throws {
        for i in 0...7 {
            await self.queue.add {
                await self.store.started(i)
                try! await Task.sleep(for: .microseconds(1000*i))
                await self.store.ended(i)
            }
        }

        try! await self.queue.waitForAll()
     
        let (starts, ends, _) = await self.store.data
        
        for i in 0...7 {
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
    
    func testAddAndWaitCancellationWhileRunning() async throws {
        
        
        let task = Task {
            await self.store.started(0)
            try await self.queue.addAndWait {
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
                try? await Task.sleep(for: 0.2)
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
            }
  
            await self.store.ended(0)
            return 1
        }

        try! await Task.sleep(for: 0.1)
        task.cancel()
        let cancellationTime = Date()
        
        let result = await task.result
        
        let (starts, ends, counter) = await self.store.data
        
        XCTAssertEqual(counter, 1)
        XCTAssertLessThan(starts[0]!, cancellationTime)
        XCTAssertLessThan(ends[0]!.timeIntervalSinceReferenceDate - starts[0]!.timeIntervalSinceReferenceDate, 0.11)
        XCTAssertEqual(try! result.get(), 1)
    }
    
    func testAddAndWaitCancellationWhileQueued() async throws {
        for _ in 0..<3 {
            await self.queue.add {
                try! await Task.sleep(for: 0.2)
            }
        }
        
        let task = Task {
            await self.store.started(0)
            try await self.queue.addAndWait {
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
                await self.store.incrementCounter()
                try? await Task.sleep(for: 0.2)
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
            }
  
            await self.store.ended(0)
        }

        try! await Task.sleep(for: 0.1)
        task.cancel()
        let cancellationTime = Date()
        
        let result = await task.result
        
        let (starts, ends, counter) = await self.store.data
        
        XCTAssertEqual(counter, 0)
        XCTAssertLessThan(starts[0]!, cancellationTime)
        XCTAssertEqual(ends.count, 0)
        XCTAssertTrue(result.isCancellationResult)
    }
    
    func testAddAndWaitThrowsCancellationWhileRunning() async throws {
        
        
        let task = Task {
            await self.store.started(0)
            try await self.queue.addAndWait {
                await self.store.incrementCounter()
                try await Task.sleep(for: 0.2)
                await self.store.incrementCounter()
            }
            await self.store.ended(0)

        }

        try! await Task.sleep(for: 0.1)
        task.cancel()
        let cancellationTime = Date()
        
        let result = await task.result
        
        let (starts, ends, counter) = await self.store.data
        
        XCTAssertEqual(counter, 1)
        XCTAssertLessThan(starts[0]!, cancellationTime)
        XCTAssertEqual(ends.count, 0)
        XCTAssertTrue(result.isCancellationResult)
    }
    
    func testAddAndWaitThrowsCancellationWhileQueued() async throws {
        
        for _ in 0..<3 {
            await self.queue.add {
                try! await Task.sleep(for: 0.2)
            }
        }
        
        let task = Task {
            await self.store.started(0)
            try await self.queue.addAndWait {
                await self.store.incrementCounter()
                try await Task.sleep(for: 0.2)
                await self.store.incrementCounter()
            }
            await self.store.ended(0)

        }

        try! await Task.sleep(for: 0.1)
        task.cancel()
        let cancellationTime = Date()
        
        let result = await task.result
        
        let (starts, ends, counter) = await self.store.data
        
        XCTAssertEqual(counter, 0)
        XCTAssertLessThan(starts[0]!, cancellationTime)
        XCTAssertEqual(ends.count, 0)
        XCTAssertTrue(result.isCancellationResult)
    }
    
    func testAddAndWaitWithIdCancellationWhileRunning() async throws {
        let task = Task {
            await self.store.started(0)
            try await self.queue.addAndWait(with: 0) {
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
                try? await Task.sleep(for: 0.2)
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
            }
            await self.store.ended(0)
        }

        try! await Task.sleep(for: 0.1)
        task.cancel()
        let cancellationTime = Date()
        
        try? await task.value
        
        let (starts, ends, counter) = await self.store.data
        
        XCTAssertEqual(counter, 1)
        XCTAssertLessThan(starts[0]!, cancellationTime)
        XCTAssertEqual(ends.count, 0)
    }
    
    func testAddAndWaitWithIdCancellationWhileQueued() async throws {
        for _ in 0..<3 {
            await self.queue.add {
                try! await Task.sleep(for: 0.2)
            }
        }
        let task = Task {
            await self.store.started(0)
            try await self.queue.addAndWait(with: 0) {
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
                try? await Task.sleep(for: 0.2)
                if !Task.isCancelled {
                    await self.store.incrementCounter()
                }
            }
            await self.store.ended(0)
        }

        try! await Task.sleep(for: 0.1)
        task.cancel()
        let cancellationTime = Date()
        
        try? await task.value
        
        let (starts, ends, counter) = await self.store.data
        
        XCTAssertEqual(counter, 0)
        XCTAssertLessThan(starts[0]!, cancellationTime)
        XCTAssertEqual(ends.count, 0)
    }
}


extension Result {
    var isCancellationResult: Bool {
        switch self {
        case .success:
            return false
        case .failure(let failure):
            if failure is CancellationError {
                return true
            }
            return false
        }
    }
}
