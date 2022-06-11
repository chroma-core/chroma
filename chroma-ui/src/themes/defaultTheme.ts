import { extendTheme } from '@chakra-ui/react'

const defaultTheme = extendTheme({
  fonts: {
    body: 'Inter, system-ui, sans-serif',
    heading: 'Inter, serif',
    mono: 'Menlo, monospace',
  },
  colors: {
    ch_gray: {
      light: '#F3F5F6',
      medium: '#E3E4DF',
      medium_dark: '#C2C5B9',
      dark: '#272622',
    },
    ch_blue: '#3A76E5',
    ch_orange: '#EA5412',
    ch_red: '#EB4026',
    ch_yellow: '#EBB125',
    ch_green: '#2FB874',
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
    Modal: {
      baseStyle: {
        borderRadius: 'sm',
      },
    },
  },
})

export default defaultTheme
