{*****************************************************
twitter snowflake�㷨
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
    Ftwepoch           : Int64; //��ʼʱ���(2018-01-01)
    FworkerIdBits      : Int64; //����id��ռ��λ��
    FdatacenterIdBits  : Int64; //���ݱ�ʶid��ռ��λ��
    FmaxWorkerId       : Int64; //֧�ֵ�������id
    FmaxDatacenterId   : Int64; //֧�ֵ�������ݱ�ʶid
    FsequenceBits      : Int64; //������id��ռ��λ��
    FworkerIdShift     : Int64; //����ID������12λ
    FdatacenterIdShift : Int64; //���ݱ�ʶid������17λ(12+5)
    FtimestampLeftShift: Int64; //ʱ���������22λ(5+5+12)
    FsequenceMask      : Int64; //�������е����룬����Ϊ4095 (0b111111111111=0xfff=4095)

    FworkerId          : Int64; //����id(0~31)
    FdatacenterId      : Int64; //���ݱ�ʶid(0~31)
    Fsequence          : Int64; //����������(0~4095)
    FlastTimestamp     : Int64; //�ϴ�����ID��ʱ���
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
  Ftwepoch            := DateTimeToUnix(EncodeDate(2018, 1, 1), False); //��ʼʱ���
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
  //��������һ�����룬ֱ������µ�ʱ���
  //lastTimestamp �ϴ�����ID��ʱ���
  //return ��ǰʱ���
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
    //�����ǰʱ��С����һ��ID���ɵ�ʱ�����˵��ϵͳʱ�ӻ��˹����ʱ��Ӧ���׳��쳣
    if (timestamp < FlastTimestamp) then
    raise Exception.CreateFmt('Clock moved backwards. ' + sLineBreak +
                              'Refusing to generate id for %d milliseconds', [FlastTimestamp - timestamp]);

    //�����ͬһʱ�����ɵģ�����к���������
    if (FlastTimestamp = timestamp) then
    begin
      Fsequence := (Fsequence + 1) and FsequenceMask;
      //�������������
      if (Fsequence = 0) then //��������һ������,����µ�ʱ���
        timestamp := tilNextMillis(FlastTimestamp);
    end
    else Fsequence := 0; //ʱ����ı䣬��������������

    //�ϴ�����ID��ʱ���
    FlastTimestamp := timestamp;

    //��λ��ͨ��������ƴ��һ�����64λ��ID
    Result := ((timestamp - Ftwepoch) shl FtimestampLeftShift) //ʱ���
                or (FdatacenterId shl FdatacenterIdShift)      //���ݱ�ʶ
                or (FworkerId shl FworkerIdShift)              //����ID
                or Fsequence;                                  //���к�
  finally
    TMonitor.Exit(FLock);
  end;
end;

end.
