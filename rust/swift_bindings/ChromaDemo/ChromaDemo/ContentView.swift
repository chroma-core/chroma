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
            if isPersistentInitialized {
                try reset()
            }
            try initialize()
            isInitialized = true
            isPersistentInitialized = false
            activeMode = .ephemeral
            addLog("Switched to ephemeral mode")
            collections = []
            refreshCollections()
        } catch {
            addLog("Failed to switch to ephemeral mode: \(error)")
        }
    }
    
    func switchToPersistentMode() {
        do {
            if isInitialized {
                try reset()
            }
            let fileManager = FileManager.default
            if !fileManager.fileExists(atPath: persistentPath) {
                try fileManager.createDirectory(atPath: persistentPath, withIntermediateDirectories: true)
                addLog("Created persistent directory at: \(persistentPath)")
            }
            addLog("Using persistent storage at: \(persistentPath)")
            try initializeWithPath(path: persistentPath, allowReset: true)
            isPersistentInitialized = true
            isInitialized = false
            activeMode = .persistent
            addLog("Switched to persistent mode")
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
                    HStack(spacing: 0) {
                        ScrollView {
                            VStack(spacing: 24) {
                                headerView
                                storageModeSelector
                                
                                if activeMode == .ephemeral {
                                    EphemeralDemoView(
                                        collectionName: $collectionName,
                                        collections: $collections,
                                        isInitialized: $isInitialized,
                                        docText: $docText,
                                        docCounter: $docCounter,
                                        showingSuccess: $showingSuccess,
                                        refreshCollections: refreshCollections,
                                        addLog: addLog
                                    )
                                }
                                
                                if activeMode == .persistent {
                                    PersistentDemoView(
                                        persistentCollectionName: $persistentCollectionName,
                                        collections: $collections,
                                        isPersistentInitialized: $isPersistentInitialized,
                                        docText: $docText,
                                        docCounter: $docCounter,
                                        showingSuccess: $showingSuccess,
                                        refreshCollections: refreshCollections,
                                        persistentPath: $persistentPath,
                                        addLog: addLog
                                    )
                                }
                            }
                            .padding(.vertical)
                        }
                        .frame(width: min(500, geometry.size.width * 0.5))
                        logsView
                    }
                } else {
                    ScrollView {
                        VStack(spacing: 0) {
                            VStack(spacing: 24) {
                                headerView
                                storageModeSelector
                                
                                if activeMode == .ephemeral {
                                    EphemeralDemoView(
                                        collectionName: $collectionName,
                                        collections: $collections,
                                        isInitialized: $isInitialized,
                                        docText: $docText,
                                        docCounter: $docCounter,
                                        showingSuccess: $showingSuccess,
                                        refreshCollections: refreshCollections,
                                        addLog: addLog
                                    )
                                }
                                
                                if activeMode == .persistent {
                                    PersistentDemoView(
                                        persistentCollectionName: $persistentCollectionName,
                                        collections: $collections,
                                        isPersistentInitialized: $isPersistentInitialized,
                                        docText: $docText,
                                        docCounter: $docCounter,
                                        showingSuccess: $showingSuccess,
                                        refreshCollections: refreshCollections,
                                        persistentPath: $persistentPath,
                                        addLog: addLog
                                    )
                                }
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
