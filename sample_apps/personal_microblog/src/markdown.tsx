import Link from "next/link";

export const Strong: React.FC<React.HTMLProps<HTMLSpanElement>> = ({ children, ...props }) => {
  if (children === "@assistant") {
    return <span className="font-bold text-[var(--accent)]" {...props}>{children}</span>;
  }
  return <strong {...props}>{children}</strong>;
};

export const AnchorTag: React.FC = ({
  children,
  href,
}: React.HTMLProps<HTMLAnchorElement>) => {
  let citationMatch = href?.match(/#citation-(\d+)/);
  if (citationMatch) {
    return <sup style={{ margin: "0 0.05em" }}>{citationMatch[1]}</sup>;
  }
  const origin = window.location.origin;
  const destination = href === undefined ? '' : href;
  const isInternalLink =
    destination.startsWith('/') || destination.startsWith(origin);
  const internalDestination = destination.replace(origin, '');
  const internalLink = (
    <Link href={internalDestination} onClick={(e) => {
      e.stopPropagation();
    }}>
      {children}
    </Link>
  );
  const externalLink = (
    <a
      href={destination}
      target="_blank"
      rel="noopener noreferrer"
      onClick={(e) => {
        e.stopPropagation();
      }}
    >
      {children}
    </a>
  );

  return isInternalLink ? internalLink : externalLink;
};
