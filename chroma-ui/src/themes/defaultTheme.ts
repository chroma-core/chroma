import { extendTheme } from '@chakra-ui/react'
import { mode } from "@chakra-ui/theme-tools"

const defaultTheme = extendTheme({
  fonts: {
    body: 'Inter, system-ui, sans-serif',
    heading: 'Inter, serif',
    mono: 'IBM Plex Mono, monospace',
  },
  colors: {
    ch_gray: {
      light: "#F3F5F6",
      medium: "#E3E4DF",
      medium_dark: "#C2C5B9",
      dark: "#272622",
      black: "#0c0c0b"
    },
    ch_blue: '#3A76E5',
    ch_orange: '#EA5412',
    ch_red: '#EB4026',
    ch_yellow: '#EBB125',
    ch_green: '#2FB874',
    ch_light_blue: '#9ad1f6'
  },
  components: {
    Button: {
      baseStyle: {
        borderRadius: 'sm',
        _focus: {
          boxShadow: 'none',
        },
      },
    },
    Accordion: {
      baseStyle: {
        _focus: {
          boxShadow: 'none',
        },
      },
    },
    Modal: {
      baseStyle: {
        borderRadius: 'sm',
      },
      variants: {
        datapoint: {
          dialog: {
            position: 'absolute',
            left: "0px",
            top: "0%",
            margin: "30px",
            bottom: "0px",
            width: "60%"
          },
        },
      },
    },
    Drawer: {
      variants: {
        alwaysOpen: {
          parts: ['dialog, dialogContainer', 'dialogOverlay'],
          dialog: {
            pointerEvents: 'auto',
          },
          dialogContainer: {
            pointerEvents: 'none',
          },
          dialogOverlay: {
            pointerEvents: 'none',
          },
        },
      },
    },
    Tag: {
      variants: {
        darkMode: {
          container: {
            color: "#111",
            bg: 'ch_light_blue',
            borderColor: 'ch_light_blue'
          }
        }
      }
    }
  },

  // useful for reference
  // styles: {
  //   global: (props:any) => ({
  //     "html, body": {
  //       background: mode("green", "white")(props),  //mode(light mode color, dark mode color)
  //     },
  //   }),
  // },
})

export default defaultTheme
