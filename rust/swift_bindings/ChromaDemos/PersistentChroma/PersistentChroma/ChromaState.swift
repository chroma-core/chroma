//
//  ChromaState.swift
//  PersistentChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import Foundation
import Chroma

@Observable
final class ChromaState {
    var docText: String = ""
    var errorMessage: String? = nil
    var logs: [String] = []
    var docCounter: Int = 0
    var persistentCollectionName: String = "persistent_collection"
    var collections: [String] = []
    var isPersistentInitialized: Bool = false
    var persistentPath: String = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].path + "/chroma_data"
    var persistentQueryEmbeddingText: String = "0.1,0.2,0.3,0.4"
    var persistentIncludeFieldsText: String = "documents"
    var isShowingFolderPicker: Bool = false
}

extension ChromaState {
    
    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    func refreshCollections() {
        do {
            collections = try listCollections()
            self.addLog("Found \(collections.count) collections:")
            collections.forEach { collection in
                self.addLog("\(collection)")
            }
        } catch {
            self.addLog("Failed to list collections: \(error)")
        }
    }
    
    func initializeWithPath(path: String, allowReset: Bool = false) throws {
        try Chroma.initializeWithPath(path: path, allowReset: allowReset)
        self.addLog("Persistent Chroma initialized at path: \(path)")
    }
    
    func reset() throws {
        try Chroma.reset()
        collections = []
        isPersistentInitialized = false
        self.addLog("Chroma reset complete")
        
        // Re-initialize Chroma after reset
        try self.initializeWithPath(path: persistentPath, allowReset: true)
        isPersistentInitialized = true
        
        DispatchQueue.main.async { [weak self] in
            self?.logs.removeAll()
        }
    }
    
    func checkForPersistentData() {
        let fileManager = FileManager.default
        let dbPath = persistentPath + "/chroma.sqlite3"
        
        if fileManager.fileExists(atPath: dbPath) {
            self.addLog("Found existing persistent database at: \(dbPath)")
        }
    }
}
