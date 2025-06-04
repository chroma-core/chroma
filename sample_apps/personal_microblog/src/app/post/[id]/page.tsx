import { notFound } from "next/navigation";
import PermalinkTweetView from "@/components/permalink-tweet-view";
import { getPostById, getPostReplies, semanticSearch } from "@/actions";

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

  const [replies, relatedPosts] = await Promise.all([
    getPostReplies(id),
    semanticSearch(post.body)
  ]);

  const relatedPostsFiltered = relatedPosts
    .filter(p => p.id !== post.id)
    .filter(p => !replies.some(r => r.id === p.id));

  return <PermalinkTweetView
    post={post}
    existingReplies={replies}
    relatedPosts={relatedPostsFiltered}
  />;
}
