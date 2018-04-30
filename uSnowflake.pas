{*****************************************************
twitter snowflake算法
author HAB
******************************************************}
unit uSnowflake;

interface

uses
  System.SysUtils,
  System.DateUtils;

type
  TSnowflake = class
  private
    Ftwepoch           : Int64; //开始时间截(2018-01-01)
    FworkerIdBits      : Int64; //机器id所占的位数
    FdatacenterIdBits  : Int64; //数据标识id所占的位数
    FmaxWorkerId       : Int64; //支持的最大机器id
    FmaxDatacenterId   : Int64; //支持的最大数据标识id
    FsequenceBits      : Int64; //序列在id中占的位数
    FworkerIdShift     : Int64; //机器ID向左移12位
    FdatacenterIdShift : Int64; //数据标识id向左移17位(12+5)
    FtimestampLeftShift: Int64; //时间截向左移22位(5+5+12)
    FsequenceMask      : Int64; //生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)

    FworkerId          : Int64; //机器id(0~31)
    FdatacenterId      : Int64; //数据标识id(0~31)
    Fsequence          : Int64; //毫秒内序列(0~4095)
    FlastTimestamp     : Int64; //上次生成ID的时间截
    FLock: TObject;
    procedure DoInit;
  protected
    function tilNextMillis(AlastTimestamp: Int64): Int64;
    function timeGen: Int64;
  public
    constructor Create(const AworkerId, AdatacenterId: Int64);
    destructor Destroy; override;
    function NextID: Int64;
  end;

implementation

{ TSnowflake }

constructor TSnowflake.Create(const AworkerId, AdatacenterId: Int64);
begin
  DoInit;
  if (AworkerId > FmaxWorkerId) or (AworkerId < 0) then
    raise Exception.CreateFmt('worker Id can''t be greater than %d or less than 0', [FmaxWorkerId]);

  if (AdatacenterId > FmaxDatacenterId) or (AdatacenterId < 0) then
    raise Exception.CreateFmt('datacenter Id can''t be greater than %d or less than 0', [FmaxDatacenterId]);

  FworkerId     := AworkerId;
  FdatacenterId := AdatacenterId;
end;

destructor TSnowflake.Destroy;
begin
  FreeAndNil(FLock);
  inherited;
end;

procedure TSnowflake.DoInit;
begin
  Ftwepoch            := DateTimeToUnix(EncodeDate(2018, 1, 1), False); //开始时间截
  FworkerIdBits       := 5;
  FdatacenterIdBits   := 5;
  FmaxWorkerId        := -1 xor (-1 shl FworkerIdBits);
  FmaxDatacenterId    := -1 xor (-1 shl FdatacenterIdBits);
  FsequenceBits       := 12;
  FworkerIdShift      := FsequenceBits;
  FdatacenterIdShift  := FsequenceBits + FworkerIdBits;
  FtimestampLeftShift := FsequenceBits + FworkerIdBits + FdatacenterIdBits;
  FsequenceMask       := -1 xor (-1 shl FsequenceBits);
  Fsequence           := 0;
  FlastTimestamp      := -1;

  FLock := TObject.Create;
end;

function TSnowflake.tilNextMillis(AlastTimestamp: Int64): Int64;
var
  timestamp: Int64;
begin
  //阻塞到下一个毫秒，直到获得新的时间戳
  //lastTimestamp 上次生成ID的时间截
  //return 当前时间戳
  timestamp := timeGen;
  while (timestamp <= AlastTimestamp) do
  begin
    timestamp := timeGen;
  end;
  Result := timestamp;
end;

function TSnowflake.timeGen: Int64;
begin
  Result := DateTimeToUnix(Now, False);
end;

function TSnowflake.NextID: Int64;
var
  timestamp: Int64;
begin
  TMonitor.Enter(FLock);
  try
    timestamp := timeGen;
    //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
    if (timestamp < FlastTimestamp) then
    raise Exception.CreateFmt('Clock moved backwards. ' + sLineBreak +
                              'Refusing to generate id for %d milliseconds', [FlastTimestamp - timestamp]);

    //如果是同一时间生成的，则进行毫秒内序列
    if (FlastTimestamp = timestamp) then
    begin
      Fsequence := (Fsequence + 1) and FsequenceMask;
      //毫秒内序列溢出
      if (Fsequence = 0) then //阻塞到下一个毫秒,获得新的时间戳
        timestamp := tilNextMillis(FlastTimestamp);
    end
    else Fsequence := 0; //时间戳改变，毫秒内序列重置

    //上次生成ID的时间截
    FlastTimestamp := timestamp;

    //移位并通过或运算拼到一起组成64位的ID
    Result := ((timestamp - Ftwepoch) shl FtimestampLeftShift) //时间戳
                or (FdatacenterId shl FdatacenterIdShift)      //数据标识
                or (FworkerId shl FworkerIdShift)              //机器ID
                or Fsequence;                                  //序列号
  finally
    TMonitor.Exit(FLock);
  end;
end;

end.
