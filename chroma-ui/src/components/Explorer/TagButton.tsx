
import React, { useState } from 'react';
import {
  Badge,
  useTheme
} from '@chakra-ui/react'

interface TagButtonProps {
  tag: any
}
  
const TagButton: React.FC<TagButtonProps> = ({ tag }) => {
  const theme = useTheme();
  const [isEditing, setIsEditing] = useState(false)

  return (
    <Badge key={tag} mr={1} mb={1} fontSize='0.6em' >{tag}</Badge>
  )
}

export default TagButton
