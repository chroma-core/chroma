//
//  ContentView.swift
//  ChromaDemoApp
//
//  Created by Nicholas Arner on 5/12/25.
//

import SwiftUI
import Chroma

struct ContentView: View {
    
    var body: some View {
        VStack {
        }
        .padding()
        .onAppear {
            Task {
                do {
                    let result = try await insertHelloDoc()
                    let myresult = String(describing: result)
                } catch {
                    print("Error:", error)
                }
            }
        }
    }
}
