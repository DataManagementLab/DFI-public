#include "Type.h"
#include "SmallInt.h"
#include "Int.h"
#include "BigInt.h"
#include "UBigInt.h"
#include "Double.h"
#include "Floating.h"
#include "TinyInt.h"

Type* Type::m_instTypes[] = { //construct in same order as TypeId!
    new Type(TypeId::INVALID),
    new SmallInt(),
    new Int(),
    new BigInt(),
    new UBigInt(),
    new Floating(),
    new Double(),
    new TinyInt()
};

uint64_t Type::getTypeSize(const TypeId type_id) {
  switch (type_id) {
    case TypeId::SMALLINT:
      return 2;
    case TypeId::INT:
      return 4;
    case TypeId::BIGINT:
      return 8;
    case TypeId::BIGUINT:
      return 8;
    case TypeId::FLOAT:
      return 4;
    case TypeId::DOUBLE:
      return 8;
    case TypeId::TINYINT:
      return 1;
    default:
      break;
  }
  return 0;
}