//
//  Task+Extenions.swift
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

extension Task where Failure == Never, Success == Never {
    /// Suspends the current task for the given amount of time.
    /// - Parameters: nanoseconds: The number of nanoseconds to wait.
    /// - Throws: For example, if the task is cancelled.
    static func sleep(for seconds: TimeInterval) async throws {
        try await Task<Never, Never>.sleep(nanoseconds: UInt64(1_000_000_000 * seconds))
    }
}

extension Task where Failure == Error {
    
    /// Creates a new task that will start executing the given operation after the given delay.
    /// - Parameters: delayInterval: The time interval to wait before starting the operation.
    /// - Parameters: priority: The priority of the task.
    /// - Parameters: operation: The operation to execute after the delay.
    @discardableResult
    static func delayed(
        by delayInterval: TimeInterval,
        priority: TaskPriority? = nil,
        operation: @escaping @Sendable () async throws -> Success
    ) -> Task {
        Task(priority: priority) {
            try await Task<Never, Never>.sleep(for: delayInterval)
            return try await operation()
        }
    }
}


extension Task where Failure == Error {

    /// Dispatches the given closure on the given DispatchQueue and returns the result.
    /// - Parameters: queue: The DispatchQueue to dispatch the closure on.
    /// - Parameters: closure: The closure to execute.
    /// - Returns: The result of the closure.
    /// - Throws: If the closure throws an error.
    static func dispatch(on queue: DispatchQueue, closure: @escaping @Sendable () throws -> Success) async throws -> Success {
        return try await withCheckedThrowingContinuation { continuation in
            queue.async {
                do {
                    let result = try closure()
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Dispatches the given closure on the given DispatchQueue and returns the result.
    /// - Parameters: queue: The DispatchQueue to dispatch the closure on.
    /// - Parameters: closure: The closure to execute.
    /// - Returns: The result of the closure.
    static func dispatch(on queue: DispatchQueue, closure: @escaping @Sendable () -> Success) async -> Success {
        return await withCheckedContinuation { continuation in
            queue.async {
                let result = closure()
                continuation.resume(returning: result)
            }
        }
    }
}
