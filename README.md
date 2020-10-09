This repository contains the archived code for our paper **"Multimodal Clustering of Boolean Tensors on MapReduce: Experiments Revisited"**.


* [Dmitry I. Ignatov, Dmitry Tochilkin, Dmitry Egurnov: Multimodal Clustering of Boolean Tensors on MapReduce: Experiments Revisited. ICFCA (Supplements) 2019: 137-151](http://ceur-ws.org/Vol-2378/longBDE4.pdf) (Best Tool award at ICFCA 2019 workshops)

**Abstract**

*This paper presents further development of distributed multimodal clustering. We introduce a new version of multimodal clustering algorithm for distributed processing in Apache Hadoop on computer clusters. Its implementation allows a user to conduct clustering on data with modality greater than two. We provide time and space complexity of the algorithm and justify its relevance. The algorithm is adapted for MapReduce distributed processing model. The program implemented by means of Apache Hadoop framework is able to perform parallel computing on thousands of nodes.*

This paper continues our previos work on disributed triclustering of 3-ary Boolean tensors (or triadic formal contexts). 

* [Sergey Zudin, Dmitry V. Gnatyshak, Dmitry I. Ignatov: Putting OAC-triclustering on MapReduce. CLA 2015: 47-58](http://ceur-ws.org/Vol-1466/paper04.pdf)

To acknowledge use of the code in publications, please cite the following papers and mention this repository.

## References

1. 
```bibtex
@inproceedings{Ignatov:2019,
  author    = {Dmitry I. Ignatov and
               Dmitry Tochilkin and
               Dmitry Egurnov},
  editor    = {Diana Cristea and
               Florence Le Ber and
               Rokia Missaoui and
               L{\'{e}}onard Kwuida and
               Baris Sertkaya},
  title     = {Multimodal Clustering of Boolean Tensors on MapReduce: Experiments
               Revisited},
  booktitle = {Supplementary Proceedings of {ICFCA} 2019 Conference and Workshops,
               Frankfurt, Germany, June 25-28, 2019},
  series    = {{CEUR} Workshop Proceedings},
  volume    = {2378},
  pages     = {137--151},
  publisher = {CEUR-WS.org},
  year      = {2019},
  url       = {http://ceur-ws.org/Vol-2378/longBDE4.pdf},
}
```

2.
```bibtex
@inproceedings{Zudin:2015,
  author    = {Sergey Zudin and
               Dmitry V. Gnatyshak and
               Dmitry I. Ignatov},
  title     = {Putting OAC-triclustering on MapReduce},
  booktitle = {Proceedings of the Twelfth International Conference on Concept Lattices
               and Their Applications, Clermont-Ferrand, France, October 13-16, 2015},
  pages     = {47--58},
  year      = {2015},
  crossref  = {DBLP:conf/cla/2015},
  url       = {http://ceur-ws.org/Vol-1466/paper04.pdf}
}

@proceedings{DBLP:conf/cla/2015,
  editor    = {Sadok Ben Yahia and
               Jan Konecny},
  title     = {Proceedings of the Twelfth International Conference on Concept Lattices
               and Their Applications, Clermont-Ferrand, France, October 13-16, 2015},
  series    = {{CEUR} Workshop Proceedings},
  volume    = {1466},
  publisher = {CEUR-WS.org},
  year      = {2015}
}
```
