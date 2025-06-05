//
//  Ticket.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 05.06.25.
//


internal final class Ticket: Identifiable, Hashable, Equatable {
    static func == (lhs: Ticket, rhs: Ticket) -> Bool {
        return lhs.id == rhs.id
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}
