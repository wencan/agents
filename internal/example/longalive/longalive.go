package main

import (
	"../../../codec"
	"../../../internal"
	"google.golang.org/grpc"
	"log"
	"net"
//	"time"
	"fmt"
//	"math/rand"
	"runtime"
	"sync"
)

var context string = `
子曰：“学而时习之，不亦悦乎？有朋自远方来，不亦乐乎？人不知而不愠，不亦君子乎？”

有子曰：“其为人也孝悌而好犯上者，鲜矣。不好犯上而好作乱者，未之有也。君子务本，本立而道生。孝悌也者，其为仁之本与？”

子曰：“巧言令色，鲜矣仁。”

曾子曰：吾日三省乎吾身。为人谋而不忠乎？与朋友交而不信乎？传不习乎？

子曰：道千乘之国，敬事而信，节用而爱人，使民以时。

子曰：弟子入则孝，出则悌，谨而信，泛爱众而亲仁，行有余力，则以学文。

子夏曰：贤贤易色，事父母，能竭其力。事君，能致其身。与朋友交，言而有信。虽曰未学，吾必谓之学矣。

子曰：君子不重则不威，学则不固。主忠信，无友不如己者，过则勿惮改。

曾子曰：慎终追远，民德归厚矣。

子禽问于子贡曰：“夫子至于是邦也，必闻其政。求之与？抑与之与？”子贡曰：“夫子温良恭俭让以得之。夫子求之也，其诸异乎人之求之与？”

子曰：父在，观其志。父没，观其行。三年无改于父之道，可谓孝矣。

有子曰：礼之用，和为贵。先王之道斯为美。小大由之，有所不行。知和而和，不以礼节之，亦不可行也。

有子曰：信近于义，言可复也。恭近于礼，远耻辱也。因不失其亲，亦可宗也。

子曰：君子食无求饱，居无求安。敏于事而慎于言，就有道而正焉。可谓好学也已。

子贡曰：“贫而无谄，富而无骄。何如？”子曰：“可也。未若贫而乐，富而好礼者也。”子贡曰：“诗云：如切如磋，如琢如磨。其斯之谓与？”子曰：“赐也，始可与言诗已矣。告诸往而知来者。”

子曰：不患人之不己知，患不知人也。
`


func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	//enable snappy compress
	if c, err := codec.New("snappy"); err != nil {
		log.Println(err)
		return
	} else {
		if cc, err := codec.WithProto(c); err != nil {
			log.Println(err)
			return
		} else {
			opts = append(opts, grpc.WithCodec(cc))
		}
	}

	client, err := internal.NewClient("127.0.0.1:8080", nil, opts...)
	if err != nil {
		log.Println(err)
		return
	}

	var conn net.Conn
	conn, err = client.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		buff := make([]byte, 1024)

		for {
			n, err := conn.Read(buff)
			if err != nil {
				log.Println(err)
				return
			}

			//log.Println("Read:", string(buff[:n]))
			fmt.Println(string(buff[:n]))
		}
	}()

	sender := func() {
		for {
			//now := <- time.After(time.Millisecond * 1000)
			//now := time.Now()
			//bytes := []byte(now.String())
			bytes := []byte(blob)

			_, err := conn.Write(bytes)
			if err != nil {
				log.Println(err)
				return
			}
		}
	}

	for i:=0; i<runtime.NumCPU(); i++ {
		waitGroup.Add(1)
		go sender()
	}

	waitGroup.Wait()
}
