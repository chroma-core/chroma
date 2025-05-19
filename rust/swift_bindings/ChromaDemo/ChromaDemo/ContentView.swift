//
//  ChromaDemoApp.swift
//  ChromaDemo
//
//  Created by Nicholas Arner on 5/14/25.
//

import SwiftUI
import Chroma
import UniformTypeIdentifiers

struct ContentView: View {
    @StateObject private var contentViewRef = ContentViewRef()
    @State private var docText: String = ""
    @State private var errorMessage: String? = nil
    @State private var logs: [String] = []
    @State private var docCounter: Int = 0
    @State private var showingSuccess: Bool = false
    @State private var collectionName: String = "my_collection"
    @State private var persistentCollectionName: String = "persistent_collection"
    @State private var collections: [String] = []
    @State private var isInitialized: Bool = false
    @State private var isPersistentInitialized: Bool = false
    // Default to Documents directory
    @State private var persistentPath: String = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].path + "/chroma_data"
    @State private var activeMode: StorageMode = .none
    @State private var isShowingFolderPicker: Bool = false
    
    enum StorageMode {
        case ephemeral, persistent, none
    }

    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    func isChromaReady() -> Bool {
        if activeMode == .ephemeral {
            return isInitialized
        } else if activeMode == .persistent {
            return isPersistentInitialized
        }
        return false
    }
    
    func switchToEphemeralMode() {
        do {
            // Reset any existing Chroma instance
            if isPersistentInitialized {
                try reset()
            }
            
            // Initialize ephemeral mode
            try initialize()
            isInitialized = true
            isPersistentInitialized = false
            activeMode = .ephemeral
            addLog("Switched to ephemeral mode")
            
            // Clear old collections and refresh
            collections = []
            refreshCollections()
        } catch {
            addLog("Failed to switch to ephemeral mode: \(error)")
        }
    }
    
    func switchToPersistentMode() {
        do {
            // Reset any existing Chroma instance
            if isInitialized {
                try reset()
            }
            
            // Create directory if needed
            let fileManager = FileManager.default
            if !fileManager.fileExists(atPath: persistentPath) {
                try fileManager.createDirectory(atPath: persistentPath, withIntermediateDirectories: true)
                addLog("Created persistent directory at: \(persistentPath)")
            }
            
            // Log the full path for reference
            addLog("Using persistent storage at: \(persistentPath)")
            
            // Initialize persistent mode
            try initializeWithPath(path: persistentPath)
            isPersistentInitialized = true
            isInitialized = false
            activeMode = .persistent
            addLog("Switched to persistent mode")
            
            // Clear old collections and refresh
            collections = []
            refreshCollections()
        } catch {
            addLog("Failed to switch to persistent mode: \(error)")
        }
    }
    
    func refreshCollections() {
        guard isChromaReady() else {
            addLog("Cannot refresh collections: Chroma not initialized")
            return
        }
        
        do {
            collections = try listCollections()
            addLog("Found \(collections.count) collections in \(activeMode) mode")
        } catch {
            addLog("Failed to list collections: \(error)")
        }
    }
    
    func checkForPersistentData() {
        // Check if the directory exists and has data
        let fileManager = FileManager.default
        let dbPath = persistentPath + "/chroma.sqlite3"
        
        if fileManager.fileExists(atPath: dbPath) {
            addLog("Found existing persistent database at: \(dbPath)")
            addLog("Use 'Switch to Persistent Mode' to load existing data")
        }
    }
    
    func resetState() {
        docText = ""
        errorMessage = nil
        docCounter = 0
        showingSuccess = false
        collectionName = "my_collection"
        persistentCollectionName = "persistent_collection"
        collections = []
        isInitialized = false
        isPersistentInitialized = false
        activeMode = .none
    }
    
    var body: some View {
        GeometryReader { geometry in
            VStack(spacing: 0) {
                if geometry.size.width > 600 {
                    // iPad/Mac layout (horizontal)
                    HStack(spacing: 0) {
                        ScrollView {
                            VStack(spacing: 24) {
                                headerView
                                
                                // Storage type selector
                                storageModeSelector
                                
                                if activeMode == .ephemeral {
                                    databaseControls
                                    documentInput
                                }
                                
                                if activeMode == .persistent {
                                    persistentDatabaseControls
                                    persistentDocumentInput
                                }
                                
                                querySection
                            }
                            .padding(.vertical)
                        }
                        .frame(width: min(500, geometry.size.width * 0.5))
                        logsView
                    }
                } else {
                    // iPhone layout (vertical)
                    ScrollView {
                        VStack(spacing: 0) {
                            VStack(spacing: 24) {
                                headerView
                                
                                // Storage type selector
                                storageModeSelector
                                
                                if activeMode == .ephemeral {
                                    databaseControls
                                    documentInput
                                }
                                
                                if activeMode == .persistent {
                                    persistentDatabaseControls
                                    persistentDocumentInput
                                }
                                
                                querySection
                            }
                            .padding(.horizontal)
                            logsView
                        }
                    }
                }
            }
        }
        .environmentObject(contentViewRef)
        .onAppear {
            contentViewRef.addLog = addLog
            checkForPersistentData()
        }
    }
    
    private var headerView: some View {
        Text("Chroma Demo")
            .font(.title)
            .fontWeight(.bold)
            .multilineTextAlignment(.center)
            .padding(.top)
    }
    
    private var storageModeSelector: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Storage Mode")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                Text("Chroma can only run in one mode at a time. Select a mode to begin.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                HStack {
                    Button {
                        switchToEphemeralMode()
                    } label: {
                        Text("Switch to Ephemeral Mode")
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 8)
                    }
                    .background(activeMode == .ephemeral ? Color.accentColor : Color.gray.opacity(0.2))
                    .foregroundColor(activeMode == .ephemeral ? Color.white : Color.primary)
                    .cornerRadius(8)
                    
                    Button {
                        switchToPersistentMode()
                    } label: {
                        Text("Switch to Persistent Mode")
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 8)
                    }
                    .background(activeMode == .persistent ? Color.accentColor : Color.gray.opacity(0.2))
                    .foregroundColor(activeMode == .persistent ? Color.white : Color.primary)
                    .cornerRadius(8)
                }
                
                if let currentMode = activeMode == .none ? nil : activeMode {
                    Text("Current mode: \(currentMode == .ephemeral ? "Ephemeral" : "Persistent")")
                        .font(.caption)
                        .foregroundColor(.secondary)
                        .padding(.top, 4)
                }
            }
            .padding()
        } label: {
            Label("Select Mode", systemImage: "arrow.triangle.branch")
        }
    }
         
    private var databaseControls: some View {
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
                    errorMessage = nil
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
                    errorMessage = nil
                }
                
                ActionButton(title: "Reset Chroma") {
                    try reset()
                    resetState()
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
    }
    
    private var persistentDatabaseControls: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Persistent Database Controls")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                VStack(alignment: .leading, spacing: 8) {
                    Text("Storage path:")
                        .font(.caption)
                    
                    HStack {
                        Text(persistentPath)
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                        
                        Spacer()
                        
                        Button {
                            // Show directory picker using UIDocumentPickerViewController
                            // For simplicity, we'll use a simple solution here
                            let documentsPath = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].path
                            persistentPath = documentsPath + "/chroma_data"
                            addLog("Set path to Documents directory: \(persistentPath)")
                        } label: {
                            HStack {
                                Image(systemName: "folder.badge.plus")
                                Text("Use Documents")
                            }
                        }
                        .disabled(isPersistentInitialized)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 4)
                        .background(Color.gray.opacity(0.2))
                        .cornerRadius(8)
                    }
                }
                      
                TextField("Collection name", text: $persistentCollectionName)
                    .textFieldStyle(.roundedBorder)
                
                ActionButton(title: "Create Collection", disabled: !isPersistentInitialized) {
                    let collectionId = try createCollection(name: persistentCollectionName)
                    addLog("Persistent collection created: \(collectionId)")
                    refreshCollections()
                    errorMessage = nil
                }
                
                ActionButton(title: "List Collections", disabled: !isPersistentInitialized) {
                    refreshCollections()
                }

                ActionButton(title: "Get All Documents", disabled: !isPersistentInitialized || collections.isEmpty) {
                    if collections.isEmpty {
                        addLog("No persistent collections to get documents from")
                        return
                    }
                    
                    addLog("--- Retrieved Persistent Documents ---")
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
                    errorMessage = nil
                }
                
                Button {
                    addLog("Storage location at: \(persistentPath)")
                } label: {
                    Text("Show Storage Location")
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 8)
                }
                .background(Color.gray.opacity(0.2))
                .cornerRadius(8)
                .disabled(!isPersistentInitialized)
                
                Text("Data will persist between app launches")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            .padding()
        } label: {
            Label("Persistent Demo", systemImage: "externaldrive.fill")
        }
    }
    
    private var documentInput: some View {
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
                    errorMessage = nil
                }
            }
            .padding()
        } label: {
            Label("Ephemeral Document Input", systemImage: "doc.text")
        }
    }
    
    private var persistentDocumentInput: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Add Document (Persistent)")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                if collections.isEmpty {
                    Text("No persistent collections available")
                        .foregroundColor(.secondary)
                } else {
                    Picker("Collection", selection: $persistentCollectionName) {
                        ForEach(collections, id: \.self) { name in
                            Text(name).tag(name)
                        }
                    }
                    .pickerStyle(.menu)
                }
                
                TextField("Enter document text...", text: $docText)
                    .textFieldStyle(.roundedBorder)
                
                ActionButton(title: "Add Document", disabled: !isPersistentInitialized || docText.isEmpty || collections.isEmpty) {
                    docCounter += 1
                    let ids = ["persistent_doc\(docCounter)"]
                    let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                    let docs = [docText]
                    _ = try addDocuments(
                        collectionName: persistentCollectionName,
                        ids: ids,
                        embeddings: embeddings,
                        documents: docs
                    )
                    showingSuccess = true
                    addLog("Document added to persistent collection '\(persistentCollectionName)': \(docText)")
                    docText = ""
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                        showingSuccess = false
                    }
                    errorMessage = nil
                }
            }
            .padding()
        } label: {
            Label("Persistent Document Input", systemImage: "doc.fill")
        }
    }
    
    private var querySection: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Query Collections")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                if !isChromaReady() || collections.isEmpty {
                    Text("No collections available to query")
                        .foregroundColor(.secondary)
                } else {
                    if activeMode == .ephemeral {
                        ActionButton(title: "Query Ephemeral", disabled: !isInitialized || collections.isEmpty) {
                            // Example query embedding
                            let queryEmbedding: [Float] = [0.1, 0.2, 0.3, 0.4]
                            
                            let result = try queryCollection(
                                collectionName: collectionName,
                                queryEmbedding: queryEmbedding,
                                nResults: 1
                            )
                            
                            if result.ids.isEmpty {
                                addLog("No results found in ephemeral collection")
                            } else {
                                addLog("Ephemeral query results:")
                                for i in 0..<result.ids.count {
                                    let id = result.ids[i]
                                    let doc = result.documents[i] ?? "(no document)"
                                    addLog("ID: \(id), Doc: \(doc)")
                                }
                            }
                        }
                    } else if activeMode == .persistent {
                        ActionButton(title: "Query Persistent", disabled: !isPersistentInitialized || collections.isEmpty) {
                            // Example query embedding
                            let queryEmbedding: [Float] = [0.1, 0.2, 0.3, 0.4]
                            
                            let result = try queryCollection(
                                collectionName: persistentCollectionName,
                                queryEmbedding: queryEmbedding,
                                nResults: 1
                            )
                            
                            if result.ids.isEmpty {
                                addLog("No results found in persistent collection")
                            } else {
                                addLog("Persistent query results:")
                                for i in 0..<result.ids.count {
                                    let id = result.ids[i]
                                    let doc = result.documents[i] ?? "(no document)"
                                    addLog("ID: \(id), Doc: \(doc)")
                                }
                            }
                        }
                    }
                }
            }
            .padding()
        } label: {
            Label("Querying", systemImage: "magnifyingglass")
        }
    }

    private var logsView: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Label("Activity Log", systemImage: "list.bullet.clipboard")
                    .font(.headline)
                Spacer()
                Button("Clear") {
                    logs.removeAll()
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(Color.gray.opacity(0.2))
                .cornerRadius(8)
                .font(.caption)
            }
            .padding(.bottom, 4)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 4) {
                    ForEach(logs.reversed(), id: \.self) { log in
                        Text(log)
                            .font(.system(.body, design: .monospaced))
                            .foregroundColor(.secondary)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .padding()
        .cornerRadius(10)
        .shadow(radius: 1)
    }
}

// Simple action button with manual styling
struct ActionButton: View {
    let title: String
    var disabled: Bool = false
    let action: () throws -> Void
    @EnvironmentObject private var contentViewRef: ContentViewRef
    
    var body: some View {
        Button(action: {
            do {
                try action()
            } catch {
                contentViewRef.addLog("Action failed: \(error)")
            }
        }) {
            Text(title)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
        }
        .background(disabled ? Color.gray.opacity(0.3) : Color.accentColor)
        .foregroundColor(disabled ? Color.gray : Color.white)
        .cornerRadius(8)
        .disabled(disabled)
    }
}

class ContentViewRef: ObservableObject {
    var addLog: (String) -> Void = { _ in }
}
