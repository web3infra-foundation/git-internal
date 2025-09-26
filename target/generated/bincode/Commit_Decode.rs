impl < __Context > :: bincode :: Decode < __Context > for Commit
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            id : :: bincode :: Decode :: decode(decoder) ?, tree_id : ::
            bincode :: Decode :: decode(decoder) ?, parent_commit_ids : ::
            bincode :: Decode :: decode(decoder) ?, author : :: bincode ::
            Decode :: decode(decoder) ?, committer : :: bincode :: Decode ::
            decode(decoder) ?, message : :: bincode :: Decode ::
            decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for Commit
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            id : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, tree_id : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?, parent_commit_ids : ::
            bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, author : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?, committer : :: bincode
            :: BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
            message : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?,
        })
    }
}