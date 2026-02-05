use crate::candidate_pipeline::candidate::PostCandidate;
use crate::candidate_pipeline::query::ScoredPostsQuery;
use crate::util::candidates_util::get_related_post_ids;
use std::collections::HashSet;
use tonic::async_trait;
use xai_candidate_pipeline::filter::{Filter, FilterResult};

pub struct PreviouslyServedPostsFilter;

#[async_trait]
impl Filter<ScoredPostsQuery, PostCandidate> for PreviouslyServedPostsFilter {
    fn enable(&self, query: &ScoredPostsQuery) -> bool {
        query.is_bottom_request
    }

    async fn filter(
        &self,
        query: &ScoredPostsQuery,
        candidates: Vec<PostCandidate>,
    ) -> Result<FilterResult<PostCandidate>, String> {
        let served_set: HashSet<i64> = query.served_ids.iter().copied().collect();
        let (removed, kept): (Vec<_>, Vec<_>) = candidates.into_iter().partition(|c| {
            get_related_post_ids(c)
                .iter()
                .any(|id| served_set.contains(id))
        });

        Ok(FilterResult { kept, removed })
    }
}
