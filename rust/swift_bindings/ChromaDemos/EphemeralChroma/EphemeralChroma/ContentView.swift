//
//  ContentView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma


struct ContentView: View {
    
    @State var state: ChromaState = .init()
    
    var body: some View {
        GeometryReader { geometry in
            VStack(spacing: 0) {
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
                        // Great solution for smaller Mac windows
                        .frame(width: min(500, geometry.size.width * 0.5))
                        logsView
                    }
                } else {
                    ScrollView {
                        VStack(spacing: 0) {
                            VStack(spacing: 24) {
                                headerView
                                databaseControls
                                documentInputSection
                                querySection
                            }
                            .padding(.horizontal)
                            logsView
                        }
                    }
                }
            }
        } // GeometryReader
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
        .overlay {
            if state.showingSuccess {
                SuccessToast()
                    .transition(.move(edge: .top).combined(with: .opacity))
            }
        }
        .animation(.easeInOut, value: state.showingSuccess)
    }
    
    var headerView: some View {
        HeaderView(title: "Ephemeral Chroma Demo")
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
