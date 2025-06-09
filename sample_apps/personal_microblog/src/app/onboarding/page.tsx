"use client";

import { useRouter } from "next/navigation";
import Logo from "@/components/ui/common/logo";

export default function OnboardingPage() {
  const router = useRouter();

  const handleGetStarted = () => {
    // Set cookie to mark user as having seen onboarding
    // Using a long expiration date (1 year)
    const expiryDate = new Date();
    expiryDate.setFullYear(expiryDate.getFullYear() + 1);
    document.cookie = `hasSeenOnboarding=true; expires=${expiryDate.toUTCString()}; path=/`;

    // Redirect to home
    router.push("/");
  };

  return (
    <div className="flex items-center justify-center py-8">
      <div className="max-w-2xl mx-auto px-6 text-center">
        <div className="mb-6 flex justify-center">
          <Logo size={40} />
        </div>

        <h1 className="text-3xl font-bold text-gray-900 mb-4">
          Welcome to Your Personal Microblog
        </h1>

        <p className="text-base text-gray-600 mb-6 leading-relaxed">
          This is your personal space to capture thoughts, ideas, and memories.
          Your AI assistant is here to help you remember and organize everything you share.
        </p>

        <div className="bg-gray-50 rounded-lg p-4 mb-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-3">
            How it works:
          </h2>
          <div className="space-y-2 text-left">
            <div className="flex items-start gap-3">
              <span className="flex-shrink-0 w-5 h-5 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-xs font-semibold">1</span>
              <p className="text-sm text-gray-700">Share your thoughts, experiences, or anything you want to remember</p>
            </div>
            <div className="flex items-start gap-3">
              <span className="flex-shrink-0 w-5 h-5 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-xs font-semibold">2</span>
              <p className="text-sm text-gray-700">Your AI assistant will respond and help organize your content</p>
            </div>
            <div className="flex items-start gap-3">
              <span className="flex-shrink-0 w-5 h-5 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-xs font-semibold">3</span>
              <p className="text-sm text-gray-700">Mention <span className="font-bold text-[var(--accent)]">@assistant</span> anytime you need help remembering something</p>
            </div>
          </div>
        </div>

        <button
          onClick={handleGetStarted}
          className="bg-black text-white px-8 py-3 rounded-lg font-medium hover:bg-gray-800 transition-colors"
        >
          Get Started
        </button>
      </div>
    </div>
  );
}
