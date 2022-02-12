# camel-project-consumer
camel project consumer side project
1. data-a-jh 토픽을 읽는 aTypeReceiveRoute와 data-b-jh 토픽을 읽는 bTypeRecieveRoute 2개를 생성한다.
2개의 에이전트를 구동하며, 각각 다른 에이전트 아이디로 설정한다.
2. aTypeReceiveRoute 수신한 데이터 데이터 베이스에 저장한다.
3. bTypeReceiveRoute 수신받은 데이터 CSV 파일로 만들어 특정 폴더에 저장한다.
4. historyReceiveRoute 이력 수신 라우트로 data-history topic에서 수신받은 데이터를 historyWriteRoute로 전달한다.
5. historyWriteRoute는 위의 세 라우트의 처리 내역 및 historyReceiveRoute로 부터의 수신 이력을 저장한다. 
6. 수신받은 라우트에서 오류 발생시 카프카 커밋 진행하지 않고 계속 재시도를 하도록 구현해야 한다. (토픽, 오프셋) 값이 있는 pass.csv 파일을 생성하여 pass 폴더에 들어가도록 하고
이 폴더에 들어간 파일을 매뉴얼로 check 폴더로 넣어줄 시에 해당 오프셋에 해당하는 데이터로 json 파일 생성하여 failedData 폴더에 append로 파일 생성되도록 한다.
7. 이렇게 어펜드가 이루어진 후에는 카프카 매뉴얼 커밋이 진행되어 다음 데이터를 읽을 수 있게된다.
