impl < __Context > :: bincode :: Decode < __Context > for TreeItem
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            mode : :: bincode :: Decode :: decode(decoder) ?, id : :: bincode
            :: Decode :: decode(decoder) ?, name : :: bincode :: Decode ::
            decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for TreeItem
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            mode : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, id : :: bincode :: BorrowDecode ::< '_,
            __Context >:: borrow_decode(decoder) ?, name : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}