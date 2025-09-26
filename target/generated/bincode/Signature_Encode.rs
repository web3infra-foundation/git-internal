impl :: bincode :: Encode for Signature
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.signature_type, encoder) ?; ::
        bincode :: Encode :: encode(&self.name, encoder) ?; :: bincode ::
        Encode :: encode(&self.email, encoder) ?; :: bincode :: Encode ::
        encode(&self.timestamp, encoder) ?; :: bincode :: Encode ::
        encode(&self.timezone, encoder) ?; core :: result :: Result :: Ok(())
    }
}