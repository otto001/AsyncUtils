//
//  AsyncSemaphore.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 05.06.25.
//

import DequeModule

actor AsyncSemaphore {
    
    
    private struct BlockedWaiter: Sendable {
        var isCancelled: Bool = false
        var continuation: CheckedContinuation<Void, Error>?
        
        init(continuation: CheckedContinuation<Void, Error>? = nil) {
            self.continuation = continuation
        }
        
        mutating func cancel() {
            guard !isCancelled else { return }
            isCancelled = true
            continuation?.resume(throwing: CancellationError())
        }
    }
    
    private var value: Int = 0
    private var blockedWaiters: [Ticket: BlockedWaiter] = [:]
    private var queue: Deque<Ticket> = []
    
    init(value: Int) {
        self.value = value
    }
    
    private func cancelBlockedWaiter(_ ticket: Ticket) {
        if let queueIndex = queue.firstIndex(of: ticket) {
            queue.remove(at: queueIndex)
        }
        blockedWaiters[ticket]?.cancel()
        blockedWaiters.removeValue(forKey: ticket)
    }
    
    public func wait() async throws {
        try Task.checkCancellation()
        
        guard value == 0 else {
            value -= 1
            return
        }
        
        let ticket = Ticket()
        blockedWaiters[ticket] = .init()
        
        return try await withTaskCancellationHandler {
            return try await withCheckedThrowingContinuation { continuation in
                #if DEBUG
                self.assertIsolated()
                #endif
                guard blockedWaiters[ticket]?.isCancelled == false else {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                blockedWaiters[ticket]?.continuation = continuation
                queue.append(ticket)
            }
        } onCancel: {
            Task {
                await self.cancelBlockedWaiter(ticket)
            }
        }
    }
    
    @discardableResult
    public func signal() -> Bool {
        value += 1
        guard let firstTicket = queue.popFirst() else {
            return false
        }
        guard let waiter = blockedWaiters[firstTicket] else {
            preconditionFailure("Missing waiter for ticket \(firstTicket)")
        }
        blockedWaiters.removeValue(forKey: firstTicket)
        guard !waiter.isCancelled else {
            preconditionFailure("Cancelled waiter for ticket \(firstTicket) should not have been in the queue")
        }
        waiter.continuation?.resume(returning: ())
        return true
    }
    
    public func run<T>(_ action: () async throws -> T) async throws -> T {
        try await self.wait()
       
        do {
            let result = try await action()
            self.signal()
            return result
        } catch {
            self.signal()
            throw error
        }
    }
}
