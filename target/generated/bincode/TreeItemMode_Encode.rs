impl :: bincode :: Encode for TreeItemMode
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        match self
        {
            Self ::Blob
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (0u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            }, Self ::BlobExecutable
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (1u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            }, Self ::Tree
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (2u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            }, Self ::Commit
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (3u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            }, Self ::Link
            =>{
                < u32 as :: bincode :: Encode >:: encode(& (4u32), encoder) ?
                ; core :: result :: Result :: Ok(())
            },
        }
    }
}