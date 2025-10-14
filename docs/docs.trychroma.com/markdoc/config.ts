import type { Config } from "@markdoc/markdoc";
import React from "react";
import InlineCode from "@/components/markdoc/inline-code";
import CodeBlock from "@/components/markdoc/code-block";
import TabbedUseCaseCodeBlock from "@/components/markdoc/tabbed-use-case-code-block";
import Tab, { CustomTabs, Tabs } from "@/components/markdoc/tabs";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/markdoc/table";
import TabbedCodeBlock from "@/components/markdoc/tabbed-code-block";
import CenteredContent from "@/components/markdoc/centered-content";
import Latex from "@/components/markdoc/latex";
import Banner from "@/components/markdoc/banner";
import Heading from "@/components/markdoc/markdoc-heading";
import MarkdocImage from "@/components/markdoc/markdoc-image";
import Accordion, { AccordionItem } from "@/components/markdoc/accordion";
import Video from "@/components/markdoc/video";
import ComboboxSteps, {
  ComboboxEntry,
} from "@/components/markdoc/combobox-steps";
import Steps, { Step } from "@/components/markdoc/steps";
import ImageHoverText from "@/components/markdoc/image-hover-text";
import PythonInstallation from "@/components/markdoc/python-installation";

interface MarkDocConfig extends Config {
  components?: Record<string, React.FC<any>>;
}

const markdocConfig: MarkDocConfig = {
  nodes: {
    code: {
      render: "InlineCode",
      attributes: {
        content: { type: String },
      },
    },
    fence: {
      render: "CodeBlock",
      attributes: {
        content: { type: String },
        language: { type: String },
      },
    },
    table: {
      render: "Table",
    },
    thead: {
      render: "TableHeader",
    },
    tbody: {
      render: "TableBody",
    },
    tr: {
      render: "TableRow",
    },
    th: {
      render: "TableHead",
    },
    td: {
      render: "TableCell",
    },
    heading: {
      render: "Heading",
      attributes: {
        level: { type: "Number", required: true },
        id: { type: "String", required: false },
      },
    },
  },
  tags: {
    TabbedCodeBlock: {
      render: "TabbedCodeBlock",
      selfClosing: true,
    },
    TabbedUseCaseCodeBlock: {
      render: "TabbedUseCaseCodeBlock",
      selfClosing: false,
      attributes: {
        language: {
          type: String,
          required: true,
        },
      },
    },
    Tab: {
      render: "Tab",
      selfClosing: false,
      attributes: {
        label: {
          type: String,
          required: true,
        },
      },
    },
    Tabs: {
      render: "Tabs",
      selfClosing: false,
    },
    CustomTabs: {
      render: "CustomTabs",
      selfClosing: false,
    },
    CenteredContent: {
      render: "CenteredContent",
      selfClosing: false,
    },
    Banner: {
      render: "Banner",
      attributes: {
        type: {
          type: String,
          required: true,
        },
      },
      selfClosing: false,
    },
    Latex: {
      render: "Latex",
      selfClosing: false,
    },
    MarkdocImage: {
      render: "MarkdocImage",
      selfClosing: true,
      attributes: {
        lightSrc: {
          type: String,
          required: true,
        },
        darkSrc: {
          type: String,
          required: true,
        },
        alt: {
          type: String,
          required: true,
        },
        title: {
          type: String,
          required: false,
        },
        width: {
          type: Number,
          required: false,
        },
        height: {
          type: Number,
          required: false,
        },
      },
    },
    Accordion: {
      render: "Accordion",
      selfClosing: false,
    },
    AccordionItem: {
      render: "AccordionItem",
      selfClosing: false,
      attributes: {
        label: {
          type: String,
          required: true,
        },
      },
    },
    Video: {
      render: "Video",
      selfClosing: true,
      attributes: {
        link: {
          type: String,
          required: true,
        },
        title: {
          type: String,
          required: true,
        },
      },
    },
    ComboboxSteps: {
      render: "ComboboxSteps",
      selfClosing: false,
      attributes: {
        defaultValue: {
          type: String,
          required: false,
        },
        itemType: {
          type: String,
          required: false,
        },
      },
    },
    ComboboxEntry: {
      render: "ComboboxEntry",
      selfClosing: false,
      attributes: {
        value: {
          type: String,
          required: true,
        },
        label: {
          type: String,
          required: true,
        },
      },
    },
    Steps: {
      render: "Steps",
      selfClosing: false,
    },
    Step: {
      render: "Step",
      selfClosing: false,
      attributes: {
        title: {
          type: String,
          required: false,
        },
      },
    },
    ImageHoverText: {
      render: "ImageHoverText",
      selfClosing: false,
      inline: true,
      attributes: {
        src: {
          type: String,
          required: true,
        },
      },
    },
    PythonInstallation: {
      render: "PythonInstallation",
      selfClosing: true,
      attributes: {
        packages: {
          type: String,
          required: true,
        },
      },
    },
  },
  components: {
    InlineCode,
    CodeBlock,
    TabbedCodeBlock,
    TabbedUseCaseCodeBlock,
    Tab,
    Tabs: Tabs,
    CustomTabs,
    Table,
    TableHeader,
    TableBody,
    TableRow,
    TableHead,
    TableCell,
    CenteredContent,
    Banner,
    Latex,
    Heading,
    MarkdocImage,
    Accordion,
    AccordionItem,
    Video,
    ComboboxSteps,
    ComboboxEntry,
    Steps,
    Step,
    ImageHoverText,
    PythonInstallation,
  },
};

export default markdocConfig;
