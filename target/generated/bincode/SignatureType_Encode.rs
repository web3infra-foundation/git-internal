impl :: bincode :: Encode for SignatureType
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        match self
        {
            Self ::Author
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (0u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            }, Self ::Committer
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (1u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            }, Self ::Tagger
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (2u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            },
        }
    }
}