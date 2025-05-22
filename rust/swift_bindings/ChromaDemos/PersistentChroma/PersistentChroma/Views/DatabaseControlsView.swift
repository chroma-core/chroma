//
//  DatabaseControlsView.swift
//  PersistentChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

struct DatabaseControlsView: View {
    
    @Bindable var state: ChromaState
    
    var body: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Persistent Database Controls")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                VStack(alignment: .leading, spacing: 8) {
                    Text("Storage path:")
                        .font(.caption)
                    
                    HStack {
                        Text(state.persistentPath)
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                    }
                }
                      
                TextField("Collection name", text: $state.persistentCollectionName)
                    .textFieldStyle(.roundedBorder)
                
                ActionButton(title: "Create Collection", disabled: !state.isPersistentInitialized) {
                    let collectionId = try createCollection(name: state.persistentCollectionName)
                    state.addLog("Persistent collection created: \(collectionId)")
                    state.refreshCollections()
                }
                
                ActionButton(title: "List Collections", disabled: !state.isPersistentInitialized) {
                    state.refreshCollections()
                }

                ActionButton(title: "Get All Documents", disabled: !state.isPersistentInitialized || state.collections.isEmpty) {
                    if state.collections.isEmpty {
                        state.addLog("[GetAll] No collections to get documents from")
                        return
                    }
                    state.addLog("[GetAll] --- Retrieved Documents ---")
                    Task {
                        for collection in state.collections {
                            state.addLog("[GetAll] Fetching documents for collection: \(collection)")
                            do {
                                let res = try await withCheckedThrowingContinuation { continuation in
                                    DispatchQueue.global(qos: .userInitiated).async {
                                        do {
                                            let res = try getAllDocuments(collectionName: collection)
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
                
                ActionButton(title: "Show Storage Location", disabled: !state.isPersistentInitialized) {
                    state.addLog("Storage location at: \(state.persistentPath)")
                }
                
                ActionButton(title: "Reset Chroma", disabled: !state.isPersistentInitialized) {
                    try state.reset()
                    state.addLog("System reset complete")
                }
                
                Text("Data will persist between app launches")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            .padding()
        } label: {
            Label("Persistent Demo", systemImage: "externaldrive.fill")
        }
    }
}
