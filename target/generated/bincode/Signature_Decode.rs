impl < __Context > :: bincode :: Decode < __Context > for Signature
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            signature_type : :: bincode :: Decode :: decode(decoder) ?, name :
            :: bincode :: Decode :: decode(decoder) ?, email : :: bincode ::
            Decode :: decode(decoder) ?, timestamp : :: bincode :: Decode ::
            decode(decoder) ?, timezone : :: bincode :: Decode ::
            decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for Signature
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            signature_type : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, name : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?, email : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
            timestamp : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, timezone : :: bincode :: BorrowDecode
            ::< '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}