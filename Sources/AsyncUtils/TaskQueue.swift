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

/// A task queue that can be used to run tasks concurrently with a maximum number of tasks running at the same time. Task are executed in the order they are added to the queue. Task can be provided with an ID to prevent duplicate tasks from being added to the queue.
public actor TaskQueue<TaskID: Hashable> {
  
    /// A task that can be added to the queue. The task can be provided with an ID to prevent duplicate tasks from being added to the queue.
    public class QueueableTask: Identifiable, Hashable, Equatable {
        /// The ID of the task. Used to prevent duplicate tasks from being added to the queue.
        public let taskID: TaskID?
        /// The closure that contains the work that the task should perform.
        public let closure: @Sendable () async -> Void
        
        /// The priority of the task. If nil, the default priority of the task queue is used.
        public let taskPriority: TaskPriority?
        
        /// If this task is waited on by an `addAndWait` call, this array will contain the continuations that should be resumed when the task is finished.
        fileprivate var continuations: [CheckedContinuation<Void, Never>] = []

        /// Initializes a new queueable task.
        /// - Parameters: taskID: The ID of the task. Used to prevent duplicate tasks from being added to the queue. If no ID is provided, no duplicate check is performed.
        /// - Parameters: closure: The closure that contains the work that the task should perform.
        public init(taskID: TaskID? = nil, taskPriority: TaskPriority? = nil, closure: @Sendable @escaping () async -> Void) {
            self.taskID = taskID
            self.taskPriority = taskPriority
            self.closure = closure
        }
        
        /// Checks if two queueable tasks are equal based on their ID.
        public static func == (lhs: TaskQueue.QueueableTask, rhs: TaskQueue.QueueableTask) -> Bool {
            lhs.id == rhs.id
        }
        
        public func hash(into hasher: inout Hasher) {
            hasher.combine(id)
        }
    }
    
    /// The queue that holds the tasks that are waiting to be executed. Used to maintain the order of the tasks.
    private var queue: Deque<QueueableTask> = .init()

    /// A map that holds the task IDs of the tasks that are currently in the queue. Used to prevent duplicate tasks from being added to the queue.
    private var taskIdMap: [TaskID: QueueableTask] = [:]

    /// A map that holds the tasks that are currently running. Used to keep track of the running tasks.
    private var runningTasks: [QueueableTask.ID: Task<Void, Never>] = .init()
    
    /// The priority of the tasks that are executed.
    public var defaultTaskPriority: TaskPriority?
    
    /// The number of tasks that are currently queued but not running.
    public var queuedCount: Int {
        self.queue.count
    }
    
    /// The number of tasks that are currently running.
    public var runningCount: Int {
        self.runningTasks.count
    }
    
    /// The total number of tasks that are currently queued or running.
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
    public var counts: Counts {
        .init(queued: self.queuedCount, running: self.runningCount)
    }
    
    /// The maximum number of tasks that can run concurrently. If this number is lowered while tasks are running, none of the running tasks are canceled. The new limit is applied to the next tasks that are started.
    public var maxConcurrentTasks: Int = 1 {
        didSet {
            self.startNextTasks()
        }
    }
    
    /// Initializes a new task queue.
    /// - Parameters: maxConcurrentTasks: The maximum number of tasks that can run concurrently.
    /// - Parameters: taskPriority: The priority of the tasks that are executed.
    public init(maxConcurrentTasks: Int = 1, defaultTaskPriority: TaskPriority? = nil) {
        self.maxConcurrentTasks = maxConcurrentTasks
        self.defaultTaskPriority = defaultTaskPriority
    }
    
    /// Called when a task is finished. Removes the task from the running tasks and the task ID map. Starts the next tasks.
    private func completedTask(_ queueableTask: QueueableTask) {
        for continuation in queueableTask.continuations {
            continuation.resume()
        }
        
        self.runningTasks.removeValue(forKey: queueableTask.id)
        if let taskID = queueableTask.taskID {
            self.taskIdMap.removeValue(forKey: taskID)
        }
        
        self.startNextTasks()
    }
    
    /// Starts the next task if the number of running tasks is less than the maximum number of concurrent tasks.
    /// - Returns: The next task that is started or nil if no task is started.
    @discardableResult
    private func startNextTask() -> QueueableTask? {
        guard self.runningCount < self.maxConcurrentTasks else { return nil }
        
        guard let task = self.queue.popFirst() else { return nil }
        let taskPriority = task.taskPriority ?? self.defaultTaskPriority
        self.runningTasks[task.id] = Task(priority: taskPriority) {
            await task.closure()
            self.completedTask(task)
        }

        return task
    }
    
    /// Starts the next tasks until the number of running tasks is equal to the maximum number of concurrent tasks.
    private func startNextTasks() {
        while self.startNextTask() != nil {
        }
    }
    
    /// Returns the IDs of all tasks that are currently queued or running.
    /// - Returns: The IDs of all tasks that are currently queued or running.
    private func allTaskIds() -> [QueueableTask.ID] {
        var tasksAhead = self.queue.map(\.id)
        tasksAhead.append(contentsOf: self.runningTasks.keys)
        return tasksAhead
    }
    
    /// Adds a task to the queue. If the task has an ID, the task is only added if no task with the same ID is already in the queue.
    /// - Parameters: queueableTask: The task that is added to the queue.
    public func addOrGet(_ queueableTask: QueueableTask) -> QueueableTask {
        if let taskID = queueableTask.taskID {
            if let otherQueueableTask = self.taskIdMap[taskID] {
                return otherQueueableTask
            }
            self.taskIdMap[taskID] = queueableTask
        }
        self.queue.append(queueableTask)
        self.startNextTasks()
        return queueableTask
    }
    
    /// Adds a task to the queue. If the task has an ID, the task is only added if no task with the same ID is already in the queue.
    /// - Parameters: queueableTask: The task that is added to the queue.
    public func add(_ queueableTask: QueueableTask) {
        _ = self.addOrGet(queueableTask)
    }
    
    /// Adds a task to the queue. If the task has an ID, the task is only added if no task with the same ID is already in the queue.
    /// - Parameters: id: The ID of the task. Used to prevent duplicate tasks from being added to the queue. If no ID is provided, no duplicate check is performed.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    public func add(with id: TaskID? = nil, _ closure: @Sendable @escaping () async -> Void)  {
        let queueableTask = QueueableTask(taskID: id, closure: closure)
        self.add(queueableTask)
    }


    /// Adds a task to the queue and waits for it to finish. 
    /// If an ID is provided and a task with the same ID is already running, the function waits for the task already existing task to finish and will not start a new task.
    /// If the task cannot be started directly due to the maximum number of concurrent tasks, the function waits until the task is started and finished.
    /// - Parameters: id: The ID of the task. Used to prevent duplicate tasks from being added to the queue. If no ID is provided, no duplicate check is performed.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    /// - Note: `addAndWait` can only be used in conjuction with an id for tasks that do no return a value or throw an error. Tasks that return a value or throw an error can be used with the other `addAndWait` functions, however, no ID can be provided for these tasks.
    public func addAndWait(with id: TaskID? = nil, _ closure: @Sendable @escaping () async -> Void) async {
        return await withCheckedContinuation { continuation in
            let queueableTask = self.addOrGet(QueueableTask(taskID: id, closure: closure))
            queueableTask.continuations.append(continuation)
        }
    }
    

    /// Adds a task to the queue and waits for it to finish. No task ID can be provided. If the task cannot be started directly due to the maximum number of concurrent tasks, the function waits until the task is started and finished.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    /// - Returns: The result of the task.
    /// - Note: No ID can be provided for tasks that return a value. Therefore, no duplicate check is performed.
    public func addAndWait<T>(_ closure: @Sendable @escaping () async -> T) async -> T {
        await withCheckedContinuation { continuation in
            let queueableTask = QueueableTask(taskID: nil) {
                let result = await closure()
                continuation.resume(returning: result)
            }
            self.add(queueableTask)
        }
    }
    
    /// Adds a task to the queue and waits for it to finish. No task ID can be provided. If the task cannot be started directly due to the maximum number of concurrent tasks, the function waits until the task is started and finished.
    /// - Parameters: closure: The closure that contains the work that the task should perform.
    /// - Returns: The result of the task.
    /// - Throws: The error that the task throws.
    /// - Note: No ID can be provided for tasks that return a value or throw an error. Therefore, no duplicate check is performed.
    public func addAndWait<T>(_ closure: @Sendable @escaping () async throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            let queueableTask = QueueableTask(taskID: nil) {
                do {
                    let result = try await closure()
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
            self.add(queueableTask)
        }
    }
    
    /// Waits for all tasks with the provided IDs to finish if they are currently running. If a task is not running, the function will not wait for it to finish.
    /// - Parameters: taskIDs: The IDs of the tasks that should be waited for.
    private func waitForTasksIfRunning(taskIDs: [QueueableTask.ID]) async {
        for taskId in taskIDs {
            if let task = self.runningTasks[taskId] {
                await task.value
            }
        }
    }
    
    /// Waits for all tasks that are currently queued or running to finish.
    public func waitForAll() async {
        // Store all tasks that we want to wait for
        let tasksAhead = self.allTaskIds()
        
        // Wait until a task slot opens (i.e. all tasks ahead are either running or completed)
        await self.addAndWait {}
        
        // If the following line is executed, all tasks we want to wait for are either already finished or currently running.
        // Therefore, we wait for all of the tasks we stored before to finish.
        await self.waitForTasksIfRunning(taskIDs: tasksAhead)
    }
    
    /// Cancels all tasks that are currently queued but not running. Already running tasks are not canceled.
    public func cancelQueued() {
        for queued in self.queue {
            if let taskID = queued.taskID {
                self.taskIdMap.removeValue(forKey: taskID)
            }
        }
        self.queue.removeAll()
    }
    
    /// Cancels all tasks that are currently queued or running.
    /// - Note: Since cancelling is cooperative, not all running tasks might be canceled immediately.  The function does *not* wait for all tasks to complete their cancellation.
    public func cancelAll() {
        self.cancelQueued()
        for task in self.runningTasks.values {
            task.cancel()
        }
    }
    
    /// Cancels all tasks that are currently queued or running and wait for them to complete.
    /// - Note: Since cancelling is cooperative, not all running tasks might be canceled immediately.  The function does wait for all tasks to complete their cancellation.
    public func cancelAllAndWait() async {
        self.cancelQueued()
        for task in self.runningTasks.values {
            task.cancel()
        }
        await self.waitForAll()
    }
}

typealias AnonymousTaskQueue = TaskQueue<Int>
