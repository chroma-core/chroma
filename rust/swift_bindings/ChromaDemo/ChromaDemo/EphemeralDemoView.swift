import SwiftUI
import Chroma

struct EphemeralDemoView: View {
    @Binding var collectionName: String
    @Binding var collections: [String]
    @Binding var isInitialized: Bool
    @Binding var docText: String
    @Binding var docCounter: Int
    @Binding var showingSuccess: Bool
    var refreshCollections: () -> Void
    var addLog: (String) -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            GroupBox {
                VStack(spacing: 16) {
                    Text("Ephemeral Database Controls")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
          
                    TextField("Collection name", text: $collectionName)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Create Collection", disabled: !isInitialized) {
                        let collectionId = try createCollection(name: collectionName)
                        addLog("Ephemeral collection created: \(collectionId)")
                        refreshCollections()
                    }
                    
                    ActionButton(title: "List Collections", disabled: !isInitialized) {
                        refreshCollections()
                    }

                    ActionButton(title: "Get All Documents", disabled: !isInitialized || collections.isEmpty) {
                        if collections.isEmpty {
                            addLog("No collections to get documents from")
                            return
                        }
                        
                        addLog("--- Retrieved Documents ---")
                        for collection in collections {
                            addLog("Collection: \(collection)")
                            let res = try getAllDocuments(collectionName: collection)
                            if res.ids.isEmpty {
                                addLog("  (empty)")
                            } else {
                                let pairs = zip(res.ids, res.documents).map { id, doc in
                                    "  Document ID: \(id)\n  Content: \(doc ?? "(nil)")"
                                }
                                pairs.forEach { addLog($0) }
                            }
                        }
                        addLog("--- End of Documents ---")
                    }
                    
                    ActionButton(title: "Reset Chroma") {
                        try reset()
                        addLog("System reset complete")
                    }
                    
                    Text("Data only persists while app is running")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                .padding()
            } label: {
                Label("Ephemeral Demo", systemImage: "cylinder.split")
            }
            
            GroupBox {
                VStack(spacing: 16) {
                    Text("Add Document (Ephemeral)")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    
                    if collections.isEmpty {
                        Text("No collections available")
                            .foregroundColor(.secondary)
                    } else {
                        Picker("Collection", selection: $collectionName) {
                            ForEach(collections, id: \.self) { name in
                                Text(name).tag(name)
                            }
                        }
                        .pickerStyle(.menu)
                    }
                    
                    TextField("Enter document text...", text: $docText)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Add Document", disabled: !isInitialized || docText.isEmpty || collections.isEmpty) {
                        docCounter += 1
                        let ids = ["doc\(docCounter)"]
                        let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                        let docs = [docText]
                        _ = try addDocuments(
                            collectionName: collectionName,
                            ids: ids,
                            embeddings: embeddings,
                            documents: docs
                        )
                        showingSuccess = true
                        addLog("Document added to ephemeral collection '\(collectionName)': \(docText)")
                        docText = ""
                        DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                            showingSuccess = false
                        }
                    }
                }
                .padding()
            } label: {
                Label("Ephemeral Document Input", systemImage: "doc.text")
            }
        }
    }
}
