import React from 'react';
import TeX from '@matejmazur/react-katex';

const MathComponent = ({ latexText }) => {
  return <TeX math={latexText} />
};

export default MathComponent;
