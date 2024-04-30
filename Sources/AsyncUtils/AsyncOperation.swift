//
//  AsyncOperation.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 30.04.24.
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


/// An `Operation` subclass that performs an asynchronous operation using swift concurrency.
public class AsyncOperation: Operation {
    private static let lockQueue = DispatchQueue(label: "de.mludwig.AsyncUtils.AsyncOperation.LockQueue")

    // MARK: - Operation Overrides

    /// An `AsyncOperation` is always asynchronous.
    override public var isAsynchronous: Bool { true }

    /// We have to override the `isExecuting` and `isFinished` properties to make sure that KVO notifications are sent correctly.
    private var _isExecuting: Bool = false
    override public private(set) var isExecuting: Bool {
        get {
            return Self.lockQueue.sync { self._isExecuting }
        }
        set {
            self.willChangeValue(forKey: "isExecuting")
            Self.lockQueue.sync(flags: [.barrier]) {
                self._isExecuting = newValue
            }
            self.didChangeValue(forKey: "isExecuting")
        }
    }

    private var _isFinished: Bool = false
    override public private(set) var isFinished: Bool {
        get {
            return Self.lockQueue.sync { self._isFinished }
        }
        set {
            self.willChangeValue(forKey: "isFinished")
            Self.lockQueue.sync(flags: [.barrier]) {
                self._isFinished = newValue
            }
            self.didChangeValue(forKey: "isFinished")
        }
    }
    
    private var _isCancelled: Bool = false
    override public private(set) var isCancelled: Bool {
        get {
            return Self.lockQueue.sync { self._isCancelled }
        }
        set {
            self.willChangeValue(forKey: "isCancelled")
            Self.lockQueue.sync(flags: [.barrier]) {
                self._isCancelled = newValue
            }
            self.didChangeValue(forKey: "isCancelled")
        }
    }
    
    private var _task: Task<Void, Never>? = nil
    public let operation: @Sendable () async -> Void
    
    
    public init(operation: @Sendable @escaping () async -> Void) {
        self.operation = operation
    }

    override public func start() {
        guard !self.isCancelled else {
            self.finish()
            return
        }

        self.main()
    }

    override public func main() {
        self.willChangeValue(forKey: "isExecuting")
        Self.lockQueue.sync(flags: [.barrier]) {
            self._isExecuting = true
            self._task = Task {
                await operation()
                self.finish()
            }
        }
        self.didChangeValue(forKey: "isExecuting")
    }
    
    override public func cancel() {
        self.willChangeValue(forKey: "isCancelled")
        Self.lockQueue.sync(flags: [.barrier]) {
            self._isCancelled = true
            self._task?.cancel()
        }
        self.didChangeValue(forKey: "isCancelled")
    }

    public func finish() {
        self.willChangeValue(forKey: "isExecuting")
        self.willChangeValue(forKey: "isFinished")
        Self.lockQueue.sync(flags: [.barrier]) {
            self._isExecuting = false
            self._isFinished = true
            self._task = nil
        }
        self.didChangeValue(forKey: "isExecuting")
        self.didChangeValue(forKey: "isFinished")
    }
}

public extension OperationQueue {
    @discardableResult
    func addOperation(_ operation: @Sendable @escaping () async -> Void) -> AsyncOperation {
        let asyncOperation = AsyncOperation(operation: operation)
        self.addOperation(asyncOperation)
        return asyncOperation
    }
}
