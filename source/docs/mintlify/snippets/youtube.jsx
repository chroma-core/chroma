// Lazy loading the YouTube iframe only when the div is visible on screen is a
// workaround to get the poster image to load in full hi res.
export const YouTube = ({
  src,
  title,
  allow,
  allowFullScreen = true,
  referrerPolicy,
}) => {
  const [isVisible, setIsVisible] = useState(false);
  const wrapperRef = useRef(null);

  useEffect(() => {
    const wrapper = wrapperRef.current;
    if (!wrapper) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
          observer.disconnect();
        }
      },
      { threshold: 0 }
    );

    observer.observe(wrapper);

    return () => observer.disconnect();
  }, []);

  return (
    <div ref={wrapperRef}>
      {isVisible && (
        <iframe
          src={src}
          title={title}
          allow={allow}
          className="w-full h-full"
          allowFullScreen={allowFullScreen}
          referrerPolicy={referrerPolicy}
        />
      )}
    </div>
  );
}
