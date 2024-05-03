import React, { useState } from 'react';
import { Icons } from './ui/icons';
import { useToast } from './ui/use-toast';

const CopyToClipboardButton: React.FC<{ textToCopy: string, className: string, customStyle: any}> = ({ textToCopy, className, customStyle }) => {
  const [isCopied, setIsCopied] = useState(false);

  if (customStyle === undefined) {
    customStyle = {};
  }

  const {toast} = useToast()

  const copyToClipboard = () => {
    const textArea = document.createElement('textarea');
    textArea.value = textToCopy;
    document.body.appendChild(textArea);
    textArea.select();

    try {
      document.execCommand('copy');
      setIsCopied(true);
    } catch (err) {
      console.error('Unable to copy to clipboard');
    } finally {
      document.body.removeChild(textArea);
    }

    toast({
      title: "Copied to clipboard",
    })

    setTimeout(() => {
      setIsCopied(false);
    }, 2000);
  };

  var copyText = isCopied ? 'Copied!' : 'Copy Code';

  return (
    <div className={className} style={customStyle}>
      <button onClick={copyToClipboard} className='flex items-center'>
        {isCopied ? <Icons.check  className="h-4 w-4" /> : <Icons.copy className="h-4 w-4 mr-2" />} {copyText}
      </button>
    </div>
  );
};

export default CopyToClipboardButton;
