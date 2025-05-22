//
//  ContentView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

#if canImport(UIKit)
import UIKit
extension View {
    func dismissKeyboard() {
        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
    }
}
#else
extension View {
    func dismissKeyboard() {
        // No-op for macOS
    }
}
#endif

// Helper for iOS platform check
func isIPad() -> Bool {
    #if canImport(UIKit)
    return UIDevice.current.userInterfaceIdiom == .pad
    #else
    return false
    #endif
}

func isIPhone() -> Bool {
    #if canImport(UIKit)
    return UIDevice.current.userInterfaceIdiom == .phone
    #else
    return false
    #endif
}

struct ContentView: View {
    
    @State var state: ChromaState = .init()
    @Environment(\.horizontalSizeClass) var horizontalSizeClass
    
    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width > 600 {
                HStack(spacing: 0) {
                    ScrollView {
                        VStack(spacing: 24) {
                            headerView
                            databaseControls
                            documentInputSection
                            querySection
                        }
                        .padding(.vertical)
                    }
                    .frame(width: min(500, geometry.size.width * 0.5))
                    logsView
                }
            } else {
                ZStack {
                    VStack(spacing: 0) {
                        ScrollView {
                            VStack(spacing: 24) {
                                headerView
                                databaseControls
                                documentInputSection
                                querySection
                            }
                            .padding(.horizontal)
                            .padding(.bottom, geometry.size.height * 0.3)
                        }
                    }
                    
                    VStack {
                        Spacer()
                        if isIPad() {
                            logsView
                        } else {
                            logsView
                                .frame(height: geometry.size.height * 0.3)
                        }
                    }
                }
            }
        }
        .onAppear {
            do {
                if !state.isInitialized {
                    try state.initialize()
                    state.refreshCollections()
                }
            } catch {
                state.addLog("Failed to initialize: \(error)")
            }
        }
    }
    
    var headerView: some View {
        VStack(spacing: 8) {
            Text("Ephemeral Chroma Demo")
                .font(.title)
                .bold()
        }
        .frame(maxWidth: .infinity)
        .padding()
    }
    
    var databaseControls: some View {
        DatabaseControlsView(state: state)
    }
    
    var documentInputSection: some View {
        DocumentInputSectionView(state: state)
    }
    
    var querySection: some View {
        QuerySectionView(state: state)
    }
    
    var logsView: some View {
        LogsView(logs: $state.logs)
    }
}

// Helper to parse comma-separated floats
func parseEmbedding(_ text: String) -> [Float]? {
    let parts = text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
    let values = parts.compactMap { Float($0) }
    guard values.count == parts.count, !values.isEmpty else { return nil }
    return values
}
