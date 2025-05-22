//
//  DatabaseControlsView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

struct DatabaseControlsView: View {
    
    @Bindable var state: ChromaState
    
    var collectionName: String {
        state.collectionName
    }
    
    var collections: [String] {
        state.collections
    }
    
    var isInitialized: Bool {
        state.isInitialized
    }
    
    @FocusState var focused: Bool
        
    var body: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Ephemeral Database Controls")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                TextField("Collection name", text: $state.collectionName)
                    .textFieldStyle(.roundedBorder)
                    .focused($focused)
                
                ActionButton(title: "Create Collection",
                             disabled: !isInitialized) {
                    self.focused = false
                    let collectionId = try Chroma.createCollection(name: collectionName)
                    state.addLog("Ephemeral collection created: \(collectionId)")
                    state.refreshCollections()
                }
                
                ActionButton(title: "List Collections",
                             disabled: !isInitialized) {
                    self.focused = false
                    state.refreshCollections()
                }
                
                ActionButton(title: "Get All Documents",
                             disabled: !isInitialized || collections.isEmpty) {
                    self.focused = false
                    if collections.isEmpty {
                        state.addLog("[GetAll] No collections to get documents from")
                        return
                    }
                    state.addLog("[GetAll] --- Retrieved Documents ---")
                    Task {
                        for collection in collections {
                            state.addLog("[GetAll] Fetching documents for collection: \(collection)")
                            do {
                                let res = try await withCheckedThrowingContinuation { continuation in
                                    DispatchQueue.global(qos: .userInitiated).async {
                                        do {
                                            let res = try Chroma.getAllDocuments(collectionName: collection)
                                            continuation.resume(returning: res)
                                        } catch {
                                            continuation.resume(throwing: error)
                                        }
                                    }
                                }
                                state.addLog("[GetAll] Got \(res.ids.count) documents for collection: \(collection)")
                                if res.ids.isEmpty {
                                    state.addLog("[GetAll]   (empty)")
                                } else {
                                    let pairs = zip(res.ids, res.documents).map { id, doc in
                                        "[GetAll]   Document ID: \(id)\n  Content: \(doc ?? "(nil)")"
                                    }
                                    pairs.forEach { state.addLog($0) }
                                }
                            } catch {
                                state.addLog("[GetAll] Error fetching documents for collection \(collection): \(error)")
                            }
                        }
                        state.addLog("[GetAll] --- End of Documents ---")
                    }
                }
                
                ActionButton(title: "Reset Chroma") {
                    self.focused = false
                    try state.reset()
                    state.addLog("System reset complete")
                }
                
                Text("Data only persists while app is running")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            .padding()
        } label: {
            Label("Ephemeral Demo", systemImage: "cylinder.split.1x2")
        }
    }
}
