//
//  LogToast.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/22/25.
//

import SwiftUI

struct LogToast: View {
    let message: String
    
    var body: some View {
        Text(message)
            .padding()
            .background(.thickMaterial)
            .foregroundColor(.primary)
            .cornerRadius(10)
            .padding(.top)
    }
}
