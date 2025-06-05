//
//  Ticket.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 05.06.25.
//

import Foundation

// Internally used class to represent a ticket for any asynchronous operation that requires a unique identifier.
// Uses memory allocation to ensure that each instance is unique and can be used for identification purposes.
internal final class Ticket: Identifiable, Hashable, Equatable {
#if DEBUG
    private var debugID: UUID = UUID()
#endif
    
    static func == (lhs: Ticket, rhs: Ticket) -> Bool {
        return lhs.id == rhs.id
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

#if DEBUG
extension Ticket: CustomDebugStringConvertible {
    var debugDescription: String {
        return "Ticket(\(debugID.uuidString))"
    }
}
#endif
