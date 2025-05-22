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
                        state.addLog("No persistent collections to get documents from")
                        return
                    }
                    
                    state.addLog("--- Retrieved Persistent Documents ---")
                    for collection in state.collections {
                        state.addLog("Collection: \(collection)")
                        let res = try getAllDocuments(collectionName: collection)
                        if res.ids.isEmpty {
                            state.addLog("  (empty)")
                        } else {
                            let pairs = zip(res.ids, res.documents).map { id, doc in
                                "  Document ID: \(id)\n  Content: \(doc ?? "(nil)")"
                            }
                            pairs.forEach { state.addLog($0) }
                        }
                    }
                    state.addLog("--- End of Documents ---")
                }
                
                ActionButton(title: "Show Storage Location", disabled: !state.isPersistentInitialized) {
                    state.addLog("Storage location at: \(state.persistentPath)")
                }
                
                ActionButton(title: "Reset System", disabled: !state.isPersistentInitialized) {
                    try state.reset()
                    state.refreshCollections()
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
