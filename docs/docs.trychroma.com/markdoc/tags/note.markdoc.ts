import MathComponent from "../../components/markdoc/Math";
import { Note } from "../../components/markdoc/Note";
import { Br, Underline } from "../../components/markdoc/misc";


export const note = {
  render: Note,
  attributes: {
    type: {
      type: String
    },
    title: {
        type: String
      },
  }
};

export const br = {
  render: Br,
};

export const math = {
  render: MathComponent,
  attributes: {
    latexText: {
      type: String
    },
  }
};

export const u = {
  render: Underline,
};
