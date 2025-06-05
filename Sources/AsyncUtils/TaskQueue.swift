//
//  TaskQueue.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 29.11.23.
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

/// A task queue that can be used to run tasks concurrently with a maximum number of tasks running at the same time. Task are executed in the order they are added to the queue (FIFO).
/// To provide more flexibility, tasks can be provided with a number of slots that they require. By default, a task requires 1 slot. Tasks will only be started if there are enough slots available. The TaskQueue will never violate FIFO order, so be careful when using slots to ensure that a task requiring more than 1 slot does not unintentionally block other tasks from being started. If you add a task with more slote than the maximum number of concurrent slots, it will never be started until the maximum number of concurrent slots is increased, effectively blocking the queue from processing any further tasks until the task is started or cancelled.
/// - Note: The TaskQueue is an actor, so it is thread-safe and can be used from multiple threads without any additional synchronization.
public actor TaskQueue {
    
    /// A task that can be added to the queue. The task can be provided with an ID to prevent duplicate tasks from being added to the queue.
    public struct QueueableTask {

        /// The closure that contains the work that the task should perform.
        /// - Note: Never set this variable on any QueueableTask instance that was not guaranteed to be isolated to the actor (e.g., may have been created outside of the actor).
        public fileprivate(set) var closure: @Sendable () async -> Void
        
        /// The priority of the task. If nil, the default priority of the task queue is used.
        public let taskPriority: TaskPriority?
        
        /// The number of slots that the task requires. By default, a task requires 1 slot. If a task requires more than 1 slot, it will only be started if there are enough slots available.
        public let slots: Int = 1
        
        /// A closure that can be called to cancel the continuation that is waiting for the task to finish. Since this closure resumes a continuation, it should only be called once.
        fileprivate var continuationCancelClosure: (@Sendable () -> Void)?
        
        /// A flag that indicates whether the task has been cancelled. This is used to prevent the task from being started if it has already been cancelled.
        fileprivate(set) var isCancelled: Bool = false

        /// A flag that indicates whether the continuation has been resumed. This is used to prevent the continuation from being resumed multiple times.
        fileprivate(set) var continuationResumed: Bool = false

        /// Initializes a new queueable task.
        /// - Parameters: slots: The number of slots that the task requires. By default, a task requires 1 slot.
        /// - Parameters: taskPriority: The priority of the task. If nil, the default priority of the task queue is used.
        /// - Parameters: closure: The closure that contains the work that the task should perform.
        public init(slots: Int = 1, taskPriority: TaskPriority? = nil, closure: @Sendable @escaping () async -> Void) {
            self.taskPriority = taskPriority
            self.closure = closure
        }
        
        /// Cancels the task. If the task has already been cancelled, this method does nothing.
        /// If the task has a continuation that has not been marked as resumed, the continuation is resumed with a CancellationError.
        mutating func cancel() {
            guard !isCancelled else { return }
            isCancelled = true
            if !continuationResumed, let continuationCancelClosure {
                continuationCancelClosure()
                continuationResumed = true
            }
        }
    }
    
    /// A struct used to keep track of running tasks. For performance reasons, the number of slots used is included.
    private struct RunningTask {
        /// The number of slots used by the task.
        let slots: Int
        /// The task that is currently running.
        let task: Task<Void, Never>
    }
    
    /// The queue that holds the tasks that are waiting to be executed. Used to maintain the order of the tasks.
    private var queue: Deque<Ticket> = .init()

    /// A map that holds the task IDs of the tasks that are currently in the queue. Used to prevent duplicate tasks from being added to the queue.
    private var tasks: [Ticket: QueueableTask] = [:]

    /// A map that holds the tasks that are currently running. Used to keep track of the running tasks.
    private var runningTasks: [Ticket: RunningTask] = .init()
    
    /// A map used by `waitForAll()` to keep track of semaphores that are used to wait for all tasks to finish. The key is a unique ticket that is used to identify the semaphore.
    private var waitForAllSemaphores: [Ticket: AsyncSemaphore] = [:]
    
    /// The priority of the tasks that are executed.
    public var defaultTaskPriority: TaskPriority?
    
    /// The number of tasks that are currently queued but not running.
    /// - Important: In almost all cases, you should not to access this value, as doing so will only lead to race conditions.
    public var queuedCount: Int {
        self.queue.count
    }
    
    /// The number of tasks that are currently running.
    /// - Important: In almost all cases, you should not to access this value, as doing so will only lead to race conditions.
    public var runningCount: Int {
        self.runningTasks.count
    }
    
    /// The number of slots that are currently used by running tasks.
    /// - Note: Since tasks can require more than 1 slot, this number can be greater than the number of running tasks.
    /// Even if there are tasks queued, this number may be less than the maximum number of concurrent slots when a task that requires more than 1 is be next in line to be started.
    /// - Important: In almost all cases, you should not to access this value, as doing so will only lead to race conditions.
    public var currentUsedSlots: Int {
        self.runningTasks.values.map(\.slots).reduce(0, +)
    }
    
    /// The total number of tasks that are currently queued or running.
    /// - Important: In almost all cases, you should not to access this value, as doing so will only lead to race conditions.
    public var count: Int {
        self.queuedCount + self.runningCount
    }
    
    /// The number of tasks that are currently queued or running.
    public struct Counts: Equatable {
        /// The number of tasks that are currently queued but not running.
        public let queued: Int

        /// The number of tasks that are currently running.
        public let running: Int
        
        /// The total number of tasks that are currently queued or running.
        public var count: Int { queued + running }
        
        /// Initializes a new counts struct.
        /// - Parameters: queued: The number of tasks that are currently queued but not running.
        /// - Parameters: running: The number of tasks that are currently running.
        public init(queued: Int, running: Int) {
            self.queued = queued
            self.running = running
        }
    }
    
    /// The number of tasks that are currently queued or running.
    /// - Important: In almost all cases, you should not to access this value, as doing so will only lead to race conditions.
    public var counts: Counts {
        .init(queued: self.queuedCount, running: self.runningCount)
    }
    
    /// The maximum number of tasks that can run concurrently. If this number is lowered while tasks are running, currently running tasks are not canceled. If this number is increased, the queue will start new tasks immediately if there are tasks in the queue that can be started.
    /// - Precondition: The maximum number of concurrent slots must be greater than 0.
    public var maxConcurrentSlots: Int = 1 {
        didSet {
            self.startNextTasks()
        }
    }
    
    /// Initializes a new task queue.
    /// - Parameters: maxConcurrentSlots: The maximum number of tasks that can run concurrently.
    /// - Parameters: taskPriority: The priority of the tasks that are executed.
    /// - Precondition: The maximum number of concurrent slots must be greater than 0.
    public init(maxConcurrentSlots: Int = 1, defaultTaskPriority: TaskPriority? = nil) {
        self.maxConcurrentSlots = maxConcurrentSlots
        self.defaultTaskPriority = defaultTaskPriority
    }

    
    /// Starts the next task if the number of running tasks is less than the maximum number of concurrent tasks.
    /// - Returns: The next task that is started or nil if no task is started.
    @discardableResult
    private func startNextTask() -> Bool {
        // Retrieve next task from the queue if there is one
        guard let ticket = queue.first else { return false }
        guard let task = tasks[ticket] else {
            preconditionFailure("Task Ticket not found.")
        }
        
        // Check that enough slots are available to start the task
        guard currentUsedSlots + task.slots <= maxConcurrentSlots else { return false }
        
        let taskPriority = task.taskPriority ?? defaultTaskPriority
        runningTasks[ticket] = .init(slots: task.slots, task: Task(priority: taskPriority, operation: task.closure))
        queue.removeFirst()
        return true
    }
    
    /// Starts as many tasks as possible from the queue.
    private func startNextTasks() {
        while startNextTask() {
        }
    }
    
    /// Returns the Tickets of all tasks that are currently queued or running.
    /// - Returns: The Tickets of all tasks that are currently queued or running.
    private func allTickets() -> [Ticket] {
        var tasksAhead = queue.map(\.self)
        tasksAhead.append(contentsOf: self.runningTasks.keys)
        return tasksAhead
    }
    
    /// Registers a new task with the given number of slots. This is used to create a ticket for a task that can be added to the queue later.
    /// - Parameters: slots: The number of slots that the task requires. 
    /// - Returns: A Ticket under which the task is registered. This ticket can be used to access the task later, e.g., to cancel it or to wait for it to finish.
    private func registerTask(slots: Int) -> Ticket {
        let ticket = Ticket()
        tasks[ticket] = .init(slots: slots) {
        }
        return ticket
    }

    /// Called when a task is finished. This method removes the task from the running tasks and starts the next tasks in the queue.
    /// - Parameters: ticket: The ticket of the task that is finished.
    /// - Parameters: continuationResume: A closure that is called to resume the continuation that is waiting for the task to finish. If the continuation has not been resumed yet, this closure is called to allow for it to resume.
    /// - Important: You must call this task every time a task is finished, either successfully or due to cancellation. This method will also start the next tasks in the queue if there are any.
    /// - Note: This method also signals any semaphores that are waiting for all tasks to finish, to notify them that a task has finished.
    private func completedTask(_ ticket: Ticket, _ continuationResume: @escaping () -> Void = {}) {

        // Remove the task from the running tasks
        runningTasks.removeValue(forKey: ticket)

        if let task = tasks.removeValue(forKey: ticket), !task.continuationResumed {
            // Resume the continuation if it has not been resumed yet
            continuationResume()
        }

        // Start the next tasks in the queue
        startNextTasks()
        
        // If there are semaphores waiting for all tasks to finish, signal them to notify that a task has finished.
        Task {
            for semaphore in waitForAllSemaphores.values {
                await semaphore.signal()
            }
        }
    }
    
    /// Cancels a task with the given ticket. If the task is currently running, it is cancelled and removed from the running tasks. If the task is queued but not running, it is removed from the queue.
    /// - Parameters: ticket: The ticket of the task that should be cancelled.
    /// - Note: If the task has a continuation that has not been marked as resumed, the continuation is resumed with a CancellationError.
    /// - Important: You must call this method to cancel a task that is currently running or queued. If you do not call this method, the task will continue to run until it is finished. If you cancel a task in any other way, it may lead to unexpected behavior, such as the task being resumed multiple times or the continuation not being resumed at all.
    private func cancel(_ ticket: Ticket) {

        // Remove the task from the queue or running tasks
        if let queueIndex = queue.firstIndex(of: ticket) {
            queue.remove(at: queueIndex)
        } else {
            runningTasks[ticket]?.task.cancel()
        }

        // Cancel the task and remove it from the tasks map
        tasks[ticket]?.cancel()

        if tasks[ticket]?.continuationResumed == true {
            // If the continuation has been resumed, we no longer need to keep the task in the tasks map
            tasks.removeValue(forKey: ticket)
        }
    }
    
    /// Adds a task to the queue. 
    /// - Parameters: task: The task that is added to the queue.
    /// - Parameters: ticket: The ticket under which the task is registered. This ticket can be used to access the task later, e.g., to cancel it or to wait for it to finish.
    /// - Parameters: insertAt: The index at which the task should be inserted in the queue. If nil, the task is added to the end of the queue.
    /// - Note: If a task with the same ticket is already in the queue, it is replaced by the new task.
    private func add(_ task: QueueableTask, with ticket: Ticket, insertAt: Int? = nil) {
        tasks[ticket] = task
        if let insertAt {
            queue.insert(ticket, at: min(queue.endIndex, insertAt))
        } else {
            queue.append(ticket)
        }
        startNextTasks()
    }
    
    /// Adds a task to the queue.
    /// - Parameters: slots: The number of slots that the task requires. By default, a task requires 1 slot.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    public func add(slots: Int = 1, _ closure: @Sendable @escaping () async -> Void)  {
        let ticket = Ticket()
        let wrappedClosure = { @Sendable in
            await closure()
            await self.completedTask(ticket)
        }
        add(.init(slots: slots, closure: wrappedClosure), with: ticket)
    }
    
    /// Adds a task to the queue and waits for it to finish.
    /// - Parameters: slots: The number of slots that the task requires. By default, a task requires 1 slot.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    /// - Returns: The result of the task.
    /// - Throws: A CancellationError if the task is canceled.
    public func addAndWait<T>(slots: Int = 1, _ closure: @Sendable @escaping () async -> T) async throws -> T {
        // Register a new task and retrieve a ticket for it.
        let ticket = registerTask(slots: slots)
        
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                // Retrieve the task and check if it is still valid (not cancelled)
                guard var task = tasks[ticket], !task.isCancelled else {
                    // If the task could not be retrieved (i.e. cancelled) or is cancelled, immediately resume the continuation with a CancellationError
                    continuation.resume(throwing: CancellationError())
                    return
                }
                // Set the continuation cancel closure to resume the continuation with a CancellationError if the task is cancelled
                task.continuationCancelClosure = {
                    continuation.resume(throwing: CancellationError())
                }
                // Set the closure that will be executed when the task is started
                // This closure also handles resolving the continuation when the task is completed and notifies the TaskQueue that the task is completed.
                task.closure = {
                    let result = await closure()
                    await self.completedTask(ticket) {
                        continuation.resume(returning: result)
                    }
                }
                // Add the task to the queue
                self.add(task, with: ticket)
            }
        } onCancel: {
            // When the task waiting on the result is cancelled, we cancel the task in the queue.
            Task {
                await self.cancel(ticket)
            }
        }
    }
    
    /// Adds a task to the queue and waits for it to finish.
    /// - Parameters: slots: The number of slots that the task requires. By default, a task requires 1 slot.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    /// - Returns: The result of the task.
    /// - Throws: A CancellationError if the task is canceled or any type of error thrown by the closure.
    public func addAndWait<T>(slots: Int = 1, _ closure: @Sendable @escaping () async throws -> T) async throws -> T {
        // Register a new task and retrieve a ticket for it.
        let ticket = registerTask(slots: slots)
        
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                guard var task = tasks[ticket], !task.isCancelled else {
                    // If the task could not be retrieved (i.e. cancelled) or is cancelled, immediately resume the continuation with a CancellationError
                    continuation.resume(throwing: CancellationError())
                    return
                }
                // Set the continuation cancel closure to resume the continuation with a CancellationError if the task is cancelled
                task.continuationCancelClosure = {
                    continuation.resume(throwing: CancellationError())
                }
                // Set the closure that will be executed when the task is started
                // This closure also handles resolving the continuation when the task is completed or exist with an error and notifies the TaskQueue that the task is completed.
                task.closure = {
                    do {
                        let result = try await closure()
                        await self.completedTask(ticket) {
                            continuation.resume(returning: result)
                        }
                    } catch {
                        await self.completedTask(ticket) {
                            continuation.resume(throwing: error)
                        }
                    }
                }
                
                // Add the task to the queue
                self.add(task, with: ticket)
            }
        } onCancel: {
            // When the task waiting on the result is cancelled, we cancel the task in the queue.
            Task {
                await self.cancel(ticket)
            }
        }
    }
    

    /// Blocks until all tasks that are currently queued or running finish or are cancelled.
    /// - Throws: A CancellationError on cancellation.
    public func waitForAll() async throws {
        // Store all tasks that we want to wait for
        let tasksAhead = allTickets()
        
        // Wait until a task slot opens (i.e. all tasks ahead are either running or completed)
        try await addAndWait {}
        
        // Register a semaphore using a new ticket.
        let semaphoreTicket = Ticket()
        let semaphore = AsyncSemaphore(value: 0)
        waitForAllSemaphores[semaphoreTicket] = semaphore
        defer { waitForAllSemaphores[semaphoreTicket] = nil }
        
        // Check if there are any remaining tasks that are currently running
        let remainingRunningTasks = Set(tasksAhead).intersection(runningTasks.keys)
        while !remainingRunningTasks.intersection(runningTasks.keys).isEmpty {
            // If there are any remaining running tasks, wait for the semaphore to be signaled. The semaphore will be signaled when a task is completed. After that, we check again if there are any remaining running tasks.
            // This will continue until all tasks are completed.
            try await semaphore.wait()
        }
    }
    
    /// Cancels all tasks that are currently queued but not running. Already running tasks are not canceled.
    public func cancelQueued() {
        while let ticket = queue.first {
            cancel(ticket)
        }
    }
    
    /// Cancels all tasks that are currently queued or running.
    /// - Note: Since cancelling is cooperative, not all running tasks might be canceled immediately.  The function does *not* wait for all tasks to complete their cancellation.
    public func cancelAll() {
        cancelQueued()
        for ticket in self.runningTasks.keys {
            cancel(ticket)
        }
    }
    
    /// Cancels all tasks that are currently queued or running and wait for them to complete.
    /// - Throws: A CancellationError on cancellation.
    /// - Note: Since cancelling is cooperative, not all running tasks might be canceled immediately. The function waits for all tasks to complete their cancellation.
    public func cancelAllAndWait() async throws {
        cancelAll()
        try await waitForAll()
    }
}
