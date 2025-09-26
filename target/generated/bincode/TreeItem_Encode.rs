impl :: bincode :: Encode for TreeItem
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.mode, encoder) ?; :: bincode ::
        Encode :: encode(&self.id, encoder) ?; :: bincode :: Encode ::
        encode(&self.name, encoder) ?; core :: result :: Result :: Ok(())
    }
}