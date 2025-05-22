//
//  ChromaState.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import Foundation
import Chroma

@Observable
final class ChromaState {
    var docText: String = ""
    var queryText: String = ""
    var logs: [String] = []
    var docCounter: Int = 0
    var collectionName: String = "my_collection"
    var collections: [String] = []
    var isInitialized: Bool = false
    var errorMessage: String? = nil
    var queryEmbeddingText: String = "0.1,0.2,0.3,0.4"
    var includeFieldsText: String = "documents"
    
    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    func refreshCollections() {
        guard isInitialized else {
            addLog("Cannot refresh collections: Chroma not initialized")
            return
        }
        
        do {
            collections = try listCollections()
            addLog("Found \(collections.count) collections:")
            collections.forEach { collection in
                addLog("\(collection)")
            }
        } catch {
            addLog("Failed to list collections: \(error)")
        }
    }
    
    func initialize(allowReset: Bool = true) throws {
        try Chroma.initialize(allowReset: allowReset)
        
        isInitialized = true
        addLog("Ephemeral Chroma initialized (allowReset: \(allowReset))")
    }
    
    func reset() throws {
        try Chroma.reset()
        collections = []
        isInitialized = false
        addLog("Chroma reset complete")
        
        // Re-initialize Chroma after reset
        try initialize()
        addLog("System reset complete")
        
        DispatchQueue.main.async { [weak self] in
            self?.logs.removeAll()
        }
    }
}
