impl :: bincode :: Encode for Commit
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.id, encoder) ?; :: bincode ::
        Encode :: encode(&self.tree_id, encoder) ?; :: bincode :: Encode ::
        encode(&self.parent_commit_ids, encoder) ?; :: bincode :: Encode ::
        encode(&self.author, encoder) ?; :: bincode :: Encode ::
        encode(&self.committer, encoder) ?; :: bincode :: Encode ::
        encode(&self.message, encoder) ?; core :: result :: Result :: Ok(())
    }
}