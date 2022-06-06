// @ts-nocheck

import { EditIcon } from "@chakra-ui/icons"
import { FormControl, FormLabel, Input, Stack, ButtonGroup, Button, useDisclosure, Box, Popover, PopoverTrigger, IconButton, PopoverContent, PopoverArrow, PopoverCloseButton } from "@chakra-ui/react"
import React from "react"
import  FocusLock from "react-focus-lock"
import { render } from "react-dom"

// 1. Create a text input component
const TextInput = React.forwardRef((props, ref) => {
    return (
      <FormControl>
        <FormLabel htmlFor={props.id}>{props.label}</FormLabel>
        <Input ref={ref} id={props.id} {...props} />
      </FormControl>
    )
  })
  
  // 2. Create the form
  const Form = ({ firstFieldRef, onCancel, onClose }) => {
    return (
      <Stack spacing={4}>
        <TextInput
          id='tag'
          ref={firstFieldRef}
          defaultValue='Tag'
        />
        <ButtonGroup display='flex' justifyContent='flex-end'>
          <Button variant='ghost' size='sm' colorScheme="orange" onClick={onClose}>
            Save
          </Button>
        </ButtonGroup>
      </Stack>
    )
  }
  
  // 3. Create the Popover
  // Ensure you set `closeOnBlur` prop to false so it doesn't close on outside click
  const PopoverForm = ({tagSelected}) => {
    const { onOpen, onClose, isOpen } = useDisclosure()
    const firstFieldRef = React.useRef(null)
  
    return (
      <>
        <Popover
          isOpen={isOpen}
          initialFocusRef={firstFieldRef}
          onOpen={onOpen}
          onClose={onClose}
          placement='bottom'
          closeOnBlur={true}
          closeOnEsc={true}
          strategy="fixed"
        >
          <PopoverTrigger>
            <Button variant='ghost' size='sm' colorScheme="orange" onClick={tagSelected}>Tag</Button>
            {/* <IconButton size='sm' icon={<EditIcon />} aria-label={""} /> */}
          </PopoverTrigger>
          <PopoverContent p={5}>
            <FocusLock returnFocus persistentFocus={false}>
              <PopoverArrow />
              <Form firstFieldRef={firstFieldRef} onCancel={onClose} onClose={onClose}/>
            </FocusLock>
          </PopoverContent>
        </Popover>
      </>
    )
  }
  
  export default PopoverForm