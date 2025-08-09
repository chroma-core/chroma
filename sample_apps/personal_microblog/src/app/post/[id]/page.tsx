import { notFound } from "next/navigation";
import PermalinkTweetView from "@/components/views/permalink-tweet-view";
import { getPostById, getPostReplies } from "@/actions";
import { TweetModelBase } from "@/types";
import { Suspense } from "react";
import PermalinkSkeleton from "@/components/ui/permalink-skeleton";

export default async function PostPermalinkPage({
  params,
}: {
  params: { id: string };
}) {
  const id = (await params).id;

  return <Suspense fallback={<PermalinkSkeleton />}>
    <PostPermalinkPageImpl id={id} />
  </Suspense>;
}

async function PostPermalinkPageImpl({ id }: { id: string }) {
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
