
import React, { useState } from 'react';
import {
  Tag,
  useTheme
} from '@chakra-ui/react'

interface TagButtonProps {
  tag: any
}

const TagButton: React.FC<TagButtonProps> = ({ tag }) => {
  const theme = useTheme();
  const [isEditing, setIsEditing] = useState(false)

  return (
    <Tag key={tag} mr={1} mb={1} fontSize='0.6em' >{tag}</Tag>
  )
}

export default TagButton
