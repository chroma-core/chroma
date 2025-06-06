import { notFound } from "next/navigation";
import PermalinkTweetView from "@/components/permalink-tweet-view";
import { getPostById, getPostReplies, semanticSearch } from "@/actions";
import { TweetModelBase } from "@/types";

export default async function PostPermalinkPage({
  params,
}: {
  params: { id: string };
}) {
  const id = (await params).id;
  const post = await getPostById(id);

  if (!post) {
    notFound();
  }

  const [parentPosts, replies] = await Promise.all([
    getParentPosts(post),
    getPostReplies(id),
  ]);

  return <PermalinkTweetView
    post={post}
    parentPosts={parentPosts}
    existingReplies={replies}
  />;
}

async function getParentPosts(post: TweetModelBase): Promise<TweetModelBase[]> {
  const parentPosts = [];
  let currentPost = post;
  while (currentPost.threadParentId) {
    const parentPost = await getPostById(currentPost.threadParentId);
    if (parentPost) {
      parentPosts.push(parentPost);
      currentPost = parentPost;
    } else {
      break;
    }
  }
  return parentPosts.reverse();
}
