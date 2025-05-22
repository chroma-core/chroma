import SwiftUI

struct LogToast: View {
    let message: String
    
    var body: some View {
        Text(message)
            .padding()
            .background(.thinMaterial)
            .foregroundColor(.primary)
            .cornerRadius(10)
            .padding(.top)
    }
}