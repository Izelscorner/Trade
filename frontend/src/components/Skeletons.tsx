/** Loading skeleton components */

export function CardSkeleton() {
  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5 animate-fade-in">
      <div className="flex justify-between items-start mb-4">
        <div>
          <div className="skeleton h-5 w-20 mb-2" />
          <div className="skeleton h-3 w-32" />
        </div>
        <div className="skeleton h-8 w-12 rounded-md" />
      </div>
      <div className="skeleton h-6 w-24 mb-1" />
      <div className="skeleton h-3 w-16" />
    </div>
  );
}

export function TableRowSkeleton() {
  return (
    <div className="flex items-center gap-4 px-4 py-3 border-b border-border-subtle">
      <div className="skeleton h-4 w-16" />
      <div className="skeleton h-3 w-32 flex-1" />
      <div className="skeleton h-4 w-20" />
      <div className="skeleton h-6 w-10 rounded-md" />
      <div className="skeleton h-6 w-10 rounded-md" />
    </div>
  );
}

export function ChartSkeleton() {
  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5 animate-fade-in">
      <div className="skeleton h-5 w-40 mb-4" />
      <div className="skeleton h-64 w-full rounded-lg" />
    </div>
  );
}

export function PageSkeleton() {
  return (
    <div className="space-y-6 p-6 max-w-[1400px] mx-auto">
      <div className="skeleton h-8 w-48 mb-8" />
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <CardSkeleton key={i} />
        ))}
      </div>
    </div>
  );
}
