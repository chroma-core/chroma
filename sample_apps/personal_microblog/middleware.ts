import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';


/**
 * Redirects to onboarding if user has not seen onboarding
 */
export function middleware(request: NextRequest) {
  const hasSeenOnboarding = request.cookies.get('hasSeenOnboarding');
  if (request.nextUrl.pathname === '/' && !hasSeenOnboarding) {
    return NextResponse.redirect(new URL('/onboarding', request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: '/'
};
