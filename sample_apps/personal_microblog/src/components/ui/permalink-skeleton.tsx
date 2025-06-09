export default function PermalinkSkeleton() {
  return (
    <div className="py-4 animate-pulse">
      {/* Header skeleton */}
      <div className="flex flex-row font-ui justify-between sticky top-0 bg-[var(--background)] py-4">
        <div className="h-6 w-12 bg-gray-200 rounded"></div>
        <div className="h-6 w-12 bg-gray-200 rounded"></div>
      </div>

      {/* Main post skeleton */}
      <div className="pb-12 px-4 pt-8">
        <div className="space-y-4">
          <div className="h-6 bg-gray-200 rounded w-full"></div>
          <div className="h-6 bg-gray-200 rounded w-5/6"></div>
          <div className="h-6 bg-gray-200 rounded w-4/6"></div>
          <div className="h-6 bg-gray-200 rounded w-3/4"></div>
        </div>
      </div>

      {/* Tweet prompt skeleton */}
      <div className="px-4 py-6 border border-gray-200 rounded-lg mb-8">
        <div className="h-20 bg-gray-100 rounded"></div>
        <div className="flex justify-between items-center mt-4">
          <div className="h-4 w-16 bg-gray-200 rounded"></div>
          <div className="h-8 w-16 bg-gray-200 rounded"></div>
        </div>
      </div>

      {/* Replies skeleton */}
      <div className="space-y-6">
        {[1, 2, 3].map((i) => (
          <div key={i} className="grid grid-cols-[120px_1fr]">
            <div className="flex flex-col items-end">
              <div className="font-ui pl-2 pr-4 pt-4 mt-[.0em] pb-4">
                <div className="h-4 w-16 bg-gray-200 rounded"></div>
              </div>
            </div>
            <div className="pt-4 pb-4 pl-4 pr-4 border-l-[.5px]">
              <div className="space-y-3">
                <div className="h-4 bg-gray-200 rounded w-full"></div>
                <div className="h-4 bg-gray-200 rounded w-4/5"></div>
                <div className="h-4 bg-gray-200 rounded w-3/5"></div>
              </div>
              <div className="mt-4 space-y-2">
                <div className="h-3 bg-gray-100 rounded w-3/4"></div>
                <div className="h-3 bg-gray-100 rounded w-1/2"></div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
